"""尾读采集器。

语义（at-least-once）：
  - 每个文件代（file_key = dev:inode:gN）维护三个位置：
      pos   —— 已从文件读到哪（含内存里的半行 pending）
      sent  —— 已发给下游到哪（完整行边界；断连/重启后回退到 acked）
      acked —— 下游确认到哪（唯一会落盘进 checkpoint 的位置）
  - 只有收到下游 ack 才推进 acked 并写 checkpoint。
  - 崩溃/重启后从 acked 重读重发 —— 宁可重复，不可丢失；
    重复由接收端按 record id (file_key:offset) 去重。
"""
import argparse
import fnmatch
import os
import signal
import socket
import threading
import time

from common import (FP_BYTES, TAIL_BYTES, FrameReader, append_event,
                    atomic_save_json, load_json, send_frame, sha1_hex, short)

DEBUG = os.environ.get("TC_DEBUG")


def dbg(msg, *a):
    if DEBUG:
        with open(DEBUG + ".collector", "a") as f:
            f.write("%.3f " % time.time() + (msg % a if a else msg) + "\n")

READ_CHUNK = 65536
POLL_INTERVAL = 0.1
ROTATE_GRACE = 2.0          # 被轮转走的文件静默多久后冲刷半行并关闭
MAX_INFLIGHT_BATCHES = 8    # 全局在途批次上限：背压，让采集滞后可见
BATCH_MAX_LINES = 50
CHECKPOINT_MIN_INTERVAL = 0.2
ANOMALY_CONFIRM = 0.5       # 截断/改写需连续观察到这么久才行动（网络盘 stat 会抖）
GONE_CONFIRM = 1.0          # inode 消失需持续这么久才认定（rename 与扫描竞态）


class FileState:
    __slots__ = ("dev", "inode", "gen", "key", "path", "fd",
                 "pos", "sent", "acked", "pending",
                 "fp_len", "fingerprint", "last_growth", "ended", "gone",
                 "anomaly_since", "gone_since")

    def __init__(self, dev, inode, gen, path):
        self.dev, self.inode, self.gen = dev, inode, gen
        self.key = "%x:%x:g%d" % (dev, inode, gen)
        self.path = path
        self.fd = None
        self.pos = 0            # fd 已读到的位置
        self.sent = 0           # 已发送位置（完整行边界）
        self.acked = 0          # 下游已确认位置（= checkpoint）
        self.pending = b""      # 区间 [sent, pos) 的字节：半行 + 待组批的整行
        self.fp_len = 0
        self.fingerprint = ""
        self.last_growth = time.time()
        self.ended = False      # 不再读，只等残余 ack
        self.gone = False       # 目录里已找不到该 inode（被改名走或删除）
        self.anomaly_since = None   # 首次观察到截断/改写的时刻（防抖）
        self.gone_since = None      # 首次观察不到 inode 的时刻（防抖）

    def open(self):
        if self.fd is not None:
            return
        fd = os.open(self.path, os.O_RDONLY)
        # 开完必须验明正身：网络盘的目录缓存会让 path 解析到别的 inode，
        # 不验就会拿着别人的 fd 读到天荒地老（真实丢数据案例）
        sb = os.fstat(fd)
        if (sb.st_dev, sb.st_ino) != (self.dev, self.inode):
            os.close(fd)
            raise OSError("path %s resolved to inode %x, expected %x"
                          % (self.path, sb.st_ino, self.inode))
        self.fd = fd

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    @property
    def unacked(self):
        return self.sent - self.acked


class Collector:
    def __init__(self, watch_dir, pattern, state_dir, receiver, work_dir):
        self.watch_dir = watch_dir
        self.pattern = pattern
        self.state_dir = state_dir          # 共享状态目录（事件流，dashboard 读）
        self.work_dir = work_dir            # 采集器私有目录（checkpoint）
        self.recv_host, self.recv_port = receiver
        self.files = {}                     # key -> FileState
        self.inode_index = {}               # "dev:inode" -> key
        self.sock = None
        self.sock_lock = threading.Lock()
        self.connected = threading.Event()
        self.inflight_batches = 0
        self.inflight_lock = threading.Lock()
        self.ckpt_dirty = False
        self.ckpt_last = 0.0
        self.stop_flag = False
        self.stats = {"lines_sent": 0, "lines_acked": 0, "batches_sent": 0,
                      "reconnects": 0, "restarts": 0}
        os.makedirs(work_dir, exist_ok=True)
        os.makedirs(state_dir, exist_ok=True)
        self._load_checkpoint()

    # ---------------- checkpoint ----------------

    def _ckpt_path(self):
        return os.path.join(self.work_dir, "checkpoint.json")

    def _load_checkpoint(self):
        ck = load_json(self._ckpt_path(), None)
        if not ck:
            return
        self.stats["restarts"] += 1
        for key, f in ck.get("files", {}).items():
            st = FileState(f["dev"], f["inode"], f["gen"], f["path"])
            st.key = key
            st.acked = f["acked"]
            st.pos = st.sent = st.acked     # 从未确认位置重新读
            st.fingerprint = f.get("fingerprint", "")
            st.fp_len = f.get("fp_len", 0)
            st.ended = f.get("ended", False)
            self.files[key] = st
            self.inode_index["%x:%x" % (st.dev, st.inode)] = key
        append_event(self.state_dir, "collector", "checkpoint_loaded",
                     files=len(self.files))

    def save_checkpoint(self, force=False):
        now = time.time()
        if not force and (not self.ckpt_dirty or
                          now - self.ckpt_last < CHECKPOINT_MIN_INTERVAL):
            return
        files = {}
        for key, st in self.files.items():
            tail_hash = ""
            if st.fd is not None and st.acked > 0:
                try:
                    n = min(TAIL_BYTES, st.acked)
                    tail_hash = sha1_hex(os.pread(st.fd, n, st.acked - n))
                except OSError:
                    pass
            files[key] = {"dev": st.dev, "inode": st.inode, "gen": st.gen,
                          "path": st.path, "acked": st.acked,
                          "fingerprint": st.fingerprint, "fp_len": st.fp_len,
                          "tail_hash": tail_hash, "ended": st.ended}
        atomic_save_json(self._ckpt_path(), {
            "files": files,
            "stats": self.stats, "ts": now})
        self.ckpt_dirty = False
        self.ckpt_last = now

    # ---------------- 文件发现与轮转 ----------------

    def _scan_dir(self):
        found = {}
        try:
            names = os.listdir(self.watch_dir)
        except OSError:
            return found
        for name in names:
            if not fnmatch.fnmatch(name, self.pattern):
                continue
            path = os.path.join(self.watch_dir, name)
            try:
                sb = os.stat(path)
            except OSError:
                continue
            found["%x:%x" % (sb.st_dev, sb.st_ino)] = (path, sb)
        return found

    @staticmethod
    def _fingerprint_of(path, n):
        try:
            with open(path, "rb") as f:
                return sha1_hex(f.read(n))
        except OSError:
            return ""

    def _looks_like_copy(self, path, size):
        """该路径的首块指纹是否与某个已跟踪文件一致（即是轮转副本）。"""
        for st in self.files.values():
            if st.fp_len and size >= st.fp_len and \
               self._fingerprint_of(path, st.fp_len) == st.fingerprint:
                return True
        return False

    def _adopt_rotated_copy(self, st, found):
        """copytruncate 副本认领：找一个指纹与旧内容一致的新文件，
        把状态平移过去从原偏移继续读 —— 已确认的不重读，未确认的不丢。
        尺寸检查放宽：stat 在网络盘上不可靠，内容指纹才是判据。"""
        for ikey, (path, sb) in found.items():
            if ikey in self.inode_index:
                continue
            if not st.fp_len or sb.st_size < st.fp_len:
                continue
            if self._fingerprint_of(path, st.fp_len) == st.fingerprint:
                return path, sb
        return None

    def _handle_truncate(self, st, found):
        old_size_known = st.pos
        adopt = self._adopt_rotated_copy(st, found)
        if adopt:
            path, sb = adopt
            st.close()
            st.dev, st.inode = sb.st_dev, sb.st_ino
            st.path = path
            self.inode_index["%x:%x" % (st.dev, st.inode)] = st.key
            append_event(self.state_dir, "collector", "copytruncate_adopted",
                         key=st.key, path=path, offset=st.sent)
        else:
            # 找不到轮转副本：acked 之后的字节找不回，如实上报缺口
            if old_size_known > st.acked:
                append_event(self.state_dir, "collector", "data_gap",
                             key=st.key, path=st.path, acked=st.acked,
                             lost_upto=old_size_known,
                             note="rotated copy not found")
            st.close()
            st.ended = True
            append_event(self.state_dir, "collector", "file_ended",
                         key=st.key, path=st.path, acked=st.acked)

    def _rewrite_suspected(self, st):
        """内容通道：直接读文件头与指纹比对（不依赖会撒谎的 stat）。"""
        if not st.fp_len:
            return False
        try:
            st.open()
            head = os.pread(st.fd, st.fp_len, 0)
        except OSError:
            return False
        if len(head) < st.fp_len:
            return False            # 头部还不够长，无法比对（交给截断路径）
        return sha1_hex(head) != st.fingerprint

    def _confirm_truncate(self, st, claimed_size):
        """内容通道确认截断：若 claimed_size 处还能读到字节，stat 在撒谎。"""
        try:
            st.open()
            return os.pread(st.fd, 1, claimed_size) == b""
        except OSError:
            return False

    def reconcile(self):
        found = self._scan_dir()

        # 0) ended 状态的 inode 映射 GC：文件已删除则让出 inode（可能被复用）
        for st in self.files.values():
            if st.ended:
                ikey = "%x:%x" % (st.dev, st.inode)
                if ikey not in found and self.inode_index.get(ikey) == st.key:
                    del self.inode_index[ikey]

        # 1) 已跟踪文件的现状核对
        now = time.time()
        for st in list(self.files.values()):
            if st.ended:
                continue
            ikey = "%x:%x" % (st.dev, st.inode)
            cur = found.get(ikey)
            if cur is None:
                # inode 暂时找不到不行动：rename 与目录扫描存在竞态，
                # 网络盘（9p/NFS）的 dirent 缓存也会抖 —— 持续消失才认定
                if st.gone_since is None:
                    st.gone_since = now
                elif not st.gone and now - st.gone_since > GONE_CONFIRM:
                    st.gone = True
                    append_event(self.state_dir, "collector", "file_gone",
                                 key=st.key, path=st.path,
                                 note="renamed away or deleted; fd kept")
                continue
            path, sb = cur
            if st.gone:
                st.gone = False
                append_event(self.state_dir, "collector", "file_back",
                             key=st.key, path=path)
            st.gone_since = None
            # 检测只信内容通道：stat 尺寸在 9p/NFS 上会滞后数秒并张冠李戴
            truncated = (sb.st_size < st.pos and
                         self._confirm_truncate(st, sb.st_size))
            rewritten = not truncated and self._rewrite_suspected(st)
            if truncated or rewritten:
                # 单次观察不行动：连续观察到才确认，幻影 stat 撑不过防抖窗
                if st.anomaly_since is None:
                    st.anomaly_since = now
                    continue
                if now - st.anomaly_since < ANOMALY_CONFIRM:
                    continue
                st.anomaly_since = None
                kind = "copytruncate" if truncated else "rewrite"
                append_event(self.state_dir, "collector", kind + "_detected",
                             key=st.key, path=path, pos=st.pos,
                             size=sb.st_size)
                self._handle_truncate(st, found)
                # 被截断/改写的 inode 变成新一代，从 0 重读
                nst = FileState(sb.st_dev, sb.st_ino, st.gen + 1, path)
                self.files[nst.key] = nst
                self.inode_index["%x:%x" % (nst.dev, nst.inode)] = nst.key
                append_event(self.state_dir, "collector", "new_generation",
                             key=nst.key, path=path, gen=nst.gen)
                continue
            st.anomaly_since = None
            if path != st.path:
                append_event(self.state_dir, "collector", "rename_detected",
                             key=st.key, old=st.path, new=path)
                st.path = path
                # 保留 fd：它被钉在 inode 上，rename 后照常读；
                # 按新名重开反而可能开到幻影目录项

        # 2) 新出现的文件
        for ikey, (path, sb) in found.items():
            if ikey in self.inode_index:
                continue
            # 首块指纹与已跟踪文件一致 -> 这是轮转副本，留给截断检测去认领；
            # 若当成新文件从 0 读，同一批字节会以两个 file_key 各发一遍
            if sb.st_size > 0 and self._looks_like_copy(path, sb.st_size):
                continue
            st = FileState(sb.st_dev, sb.st_ino, 0, path)
            self.files[st.key] = st
            self.inode_index[ikey] = st.key
            append_event(self.state_dir, "collector", "file_discovered",
                         key=st.key, path=path, size=sb.st_size)

        # 3) 指纹采集
        for st in self.files.values():
            if st.ended or st.fp_len:
                continue
            cur = found.get("%x:%x" % (st.dev, st.inode))
            if not cur:
                continue
            size = cur[1].st_size
            if size > 0:
                st.fp_len = min(FP_BYTES, size)
                st.fingerprint = self._fingerprint_of(cur[0], st.fp_len)
                self.ckpt_dirty = True

    # ---------------- 连接 ----------------

    def _ensure_conn(self):
        if self.sock is not None:
            return True
        try:
            s = socket.create_connection((self.recv_host, self.recv_port),
                                         timeout=3)
            s.settimeout(5)
        except OSError:
            return False
        self.sock = s
        self.connected.set()
        # 连接重建：已发未确认的全部回退，从 acked 重读重发
        with self.inflight_lock:
            self.inflight_batches = 0
        dbg("CONNECT reset inflight=0")
        for st in self.files.values():
            if st.pos != st.acked or st.pending:
                st.pos = st.sent = st.acked
                st.pending = b""
        self.stats["reconnects"] += 1
        append_event(self.state_dir, "collector", "downstream_connected",
                     resent_from="acked_offset")
        threading.Thread(target=self._ack_loop, args=(s,), daemon=True).start()
        return True

    def _drop_conn(self, why, sock=None):
        with self.sock_lock:
            if sock is not None and self.sock is not sock:
                return                      # 旧连接的线程，别关新连接
            if self.sock:
                try:
                    self.sock.close()
                except OSError:
                    pass
            self.sock = None
        self.connected.clear()
        append_event(self.state_dir, "collector", "downstream_disconnected",
                     reason=why)

    def _ack_loop(self, sock):
        reader = FrameReader(sock)
        idle_timeouts = 0
        while not self.stop_flag:
            try:
                frame = reader.read_frame()
            except socket.timeout:
                # 空闲不算断；长时间无任何帧才认为连接已死，重连重发
                idle_timeouts += 1
                if idle_timeouts >= 12:         # ~60s
                    break
                continue
            except (OSError, ValueError):
                break
            if frame is None:
                break
            idle_timeouts = 0
            if frame.get("type") == "hello":
                # 握手：接收端告知各 key 已持久化的高水位，直接推进 acked
                for key, upto in (frame.get("high_waters") or {}).items():
                    st = self.files.get(key)
                    if st and upto > st.acked:
                        st.acked = upto
                        self.ckpt_dirty = True
                continue
            if frame.get("type") == "ack":
                st = self.files.get(frame.get("key"))
                upto = frame.get("upto", 0)
                if st and upto > st.acked:
                    st.acked = upto
                    self.ckpt_dirty = True
                with self.inflight_lock:
                    self.inflight_batches = max(0, self.inflight_batches - 1)
                    nif = self.inflight_batches
                dbg("ACK key=%s upto=%d inflight=%d",
                    frame.get("key"), upto, nif)
                self.stats["lines_acked"] += frame.get("lines", 0)
        self._drop_conn("ack stream closed", sock=sock)

    # ---------------- 读取与发送 ----------------

    def _send_batch(self, st, items):
        """items: [(bytes, partial)]，从 st.sent 开始必须连续。失败返回 False。"""
        if not items:
            return True
        batch = {"type": "batch", "key": st.key, "path": st.path,
                 "gen": st.gen, "fp": short(st.fingerprint),
                 "start": st.sent,
                 "lines": [b.decode("utf-8", "replace") for b, _ in items],
                 "lens": [len(b) for b, _ in items],
                 "partial": [p for _, p in items]}
        with self.sock_lock:
            if self.sock is None:
                return False
            try:
                send_frame(self.sock, batch)
            except OSError:
                self._drop_conn("send failed")
                return False
        st.sent += sum(len(b) for b, _ in items)
        with self.inflight_lock:
            self.inflight_batches += 1
            nif = self.inflight_batches
        dbg("SEND key=%s start=%d n=%d inflight=%d",
            st.key, batch["start"], len(items), nif)
        self.stats["batches_sent"] += 1
        self.stats["lines_sent"] += len(items)
        return True

    @staticmethod
    def _split_lines(buf, base_off):
        """把 buf（起点偏移 base_off）切成完整行 + 残余半行。"""
        lines, start = [], 0
        for i, b in enumerate(buf):
            if b == 0x0A:
                lines.append(buf[start:i + 1])
                start = i + 1
        return lines, buf[start:]

    def _read_and_send_file(self, st):
        if st.ended or self.sock is None:
            return
        try:
            st.open()
            chunk = os.pread(st.fd, READ_CHUNK, st.pos)
        except OSError:
            return
        if chunk:
            st.pos += len(chunk)
            st.last_growth = time.time()
        buf = st.pending + chunk
        if not buf:
            return
        # 注意：即使无新数据（chunk 为空）也要继续冲刷 pending 里
        # 上次没发出去的完整行，否则重连重发的大批积压会卡到下次文件增长
        lines, remainder = self._split_lines(buf, st.sent)
        batch = lines[:BATCH_MAX_LINES]
        if batch and self._send_batch(st, [(b, False) for b in batch]):
            # 没发出去的完整行留回 pending（pending 不变式：== 文件[sent:pos]）
            st.pending = b"".join(lines[BATCH_MAX_LINES:]) + remainder
        else:
            st.pending = buf      # 发送失败：一字节不丢，留待重连后重发

    def _flush_before_close(self, st):
        """关闭前：先发完整行，再把残余半行作为 partial 记录发出去。"""
        lines, remainder = self._split_lines(st.pending, st.sent)
        items = [(b, False) for b in lines]
        if remainder:
            items.append((remainder, True))
        if items and self._send_batch(st, items):
            if remainder:
                append_event(self.state_dir, "collector", "partial_flushed",
                             key=st.key, path=st.path, bytes=len(remainder))
            st.pending = b""

    def _maybe_close_idle(self):
        now = time.time()
        live = self.pattern.rstrip("*")     # "app.log*" -> "app.log"
        for st in self.files.values():
            if st.ended:
                continue
            if not st.gone and os.path.basename(st.path) == live:
                continue                    # 活跃主文件：半行继续等，不关闭
            if now - st.last_growth <= ROTATE_GRACE:
                continue
            self._flush_before_close(st)
            st.close()
            st.ended = True
            self.ckpt_dirty = True
            append_event(self.state_dir, "collector", "file_closed_idle",
                         key=st.key, path=st.path, acked=st.acked)

    # ---------------- 状态导出（dashboard 用） ----------------

    def export_status(self):
        sizes = {ik: sb.st_size for ik, (_, sb) in self._scan_dir().items()}
        files = []
        for st in self.files.values():
            size = sizes.get("%x:%x" % (st.dev, st.inode))
            files.append({
                "key": st.key, "path": st.path, "gen": st.gen,
                "fingerprint": st.fingerprint, "fp_len": st.fp_len,
                "pos": st.pos, "sent": st.sent, "acked": st.acked,
                "pending": len(st.pending), "size": size,
                # 积压 = 已读未确认（内容通道可信）；stat 尺寸在网络盘上不可信
                "lag": st.pos - st.acked,
                "unacked": st.unacked, "ended": st.ended, "gone": st.gone})
        status = {"ts": time.time(), "stats": self.stats,
                  "connected": self.connected.is_set(),
                  "inflight_batches": self.inflight_batches,
                  "files": files}
        atomic_save_json(os.path.join(self.work_dir, "status.json"), status)

    # ---------------- 主循环 ----------------

    def run(self):
        append_event(self.state_dir, "collector", "collector_started",
                     pid=os.getpid(), dir=self.watch_dir,
                     pattern=self.pattern)
        while not self.stop_flag:
            self.reconcile()
            if self._ensure_conn():
                with self.inflight_lock:
                    room = MAX_INFLIGHT_BATCHES - self.inflight_batches
                if room <= 0:
                    dbg("STALL inflight=%d", self.inflight_batches)
                if room > 0:
                    for st in list(self.files.values()):
                        if room <= 0:
                            break
                        if not st.ended:
                            self._read_and_send_file(st)
                            room -= 1
            self._maybe_close_idle()
            self.save_checkpoint()
            self.export_status()
            time.sleep(POLL_INTERVAL)
        self.save_checkpoint(force=True)
        self.export_status()
        append_event(self.state_dir, "collector", "collector_stopped",
                     pid=os.getpid())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--pattern", default="app.log*")
    ap.add_argument("--state", required=True, help="共享状态目录（事件流）")
    ap.add_argument("--work", required=True, help="采集器私有目录（checkpoint）")
    ap.add_argument("--receiver", default="127.0.0.1:9099")
    args = ap.parse_args()
    host, port = args.receiver.rsplit(":", 1)
    c = Collector(args.dir, args.pattern, args.state, (host, int(port)),
                  args.work)

    def _term(*_):
        c.stop_flag = True
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    c.run()


if __name__ == "__main__":
    main()
