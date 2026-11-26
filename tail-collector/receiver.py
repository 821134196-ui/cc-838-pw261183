"""下游接收端。

确认语义：
  1. 收到批次 -> 按 record id (file_key:offset) 去重
  2. 新记录追加进 journal 并 fsync —— 先落盘
  3. 再回 ack {key, upto=高水位} —— 后确认
  采集端只有收到 ack 才推进 checkpoint，因此“已确认”必然“已持久化”。

故障注入（控制文件，运行时可改）：
  {"delay_ms": 200}        每批处理前睡眠，模拟下游慢 -> 采集端背压滞后
  {"drop_next_ack": true}  落盘后不回 ack 直接断连 —— 复现“收到了但确认丢失”
"""
import argparse
import json
import os as _os
_DEBUG = _os.environ.get("TC_DEBUG")
def dbg(msg, *a):
    if _DEBUG:
        with open(_DEBUG + ".receiver", "a") as f:
            f.write("%.3f " % time.time() + (msg % a if a else msg) + "\n")
import os
import signal
import socket
import threading
import time

from common import (FrameReader, append_event, atomic_save_json, load_json,
                    send_frame)


class KeyState:
    __slots__ = ("intervals", "high_water", "unique", "dups", "received")

    def __init__(self):
        self.intervals = []     # 已收到的 [start, end) 区间，按 start 有序
        self.high_water = 0     # 从 0 起连续覆盖到的位置
        self.unique = 0
        self.dups = 0
        self.received = 0

    def add(self, start, end):
        self.intervals.append([start, end])
        self.intervals.sort()
        merged = []
        for iv in self.intervals:
            if merged and iv[0] <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], iv[1])
            else:
                merged.append(list(iv))
        self.intervals = merged
        hw = 0
        for s, e in merged:
            if s <= hw:
                hw = max(hw, e)
            else:
                break
        self.high_water = hw


class Receiver:
    def __init__(self, listen, state_dir, work_dir, control_path):
        self.host, self.port = listen
        self.state_dir = state_dir
        self.work_dir = work_dir
        self.control_path = control_path
        self.keys = {}                  # key -> KeyState
        self.paths = {}                 # key -> 最近 path
        self.seen = set()               # "key:offset" 已持久化的 record id
        self.stats = {"batches": 0, "acks_sent": 0, "acks_dropped": 0,
                      "connections": 0, "dup_lines": 0}
        self.lock = threading.Lock()
        self.stop_flag = False
        os.makedirs(work_dir, exist_ok=True)
        os.makedirs(state_dir, exist_ok=True)
        self.journal_path = os.path.join(work_dir, "journal.jsonl")
        self.journal = open(self.journal_path, "ab", buffering=0)
        self._rebuild()

    # ---------------- 持久化 ----------------

    def _rebuild(self):
        """重启后从 journal 重建去重集合与高水位 —— 已确认的不会丢。"""
        try:
            with open(self.journal_path, "rb") as f:
                for raw in f:
                    try:
                        rec = json.loads(raw.decode())
                    except ValueError:
                        continue
                    key, off, ln = rec["key"], rec["offset"], rec["len"]
                    self.seen.add("%s:%d" % (key, off))
                    ks = self.keys.setdefault(key, KeyState())
                    ks.add(off, off + ln)
                    ks.unique += 1
                    ks.received += 1
                    self.paths[key] = rec.get("path", "")
        except FileNotFoundError:
            pass
        if self.keys:
            append_event(self.state_dir, "receiver", "journal_rebuilt",
                         keys=len(self.keys), records=len(self.seen))

    def _export_state(self):
        with self.lock:
            files = {k: {"high_water": ks.high_water, "unique": ks.unique,
                         "dups": ks.dups, "received": ks.received,
                         "path": self.paths.get(k, "")}
                     for k, ks in self.keys.items()}
            stats = dict(self.stats)
        atomic_save_json(os.path.join(self.work_dir, "state.json"),
                         {"ts": time.time(), "stats": stats, "files": files})

    def _control(self):
        return load_json(self.control_path, {}) or {}

    # ---------------- 批次处理 ----------------

    def _handle_batch(self, b):
        key = b["key"]
        start = b["start"]
        lines, lens, partials = b["lines"], b["lens"], b["partial"]
        new_recs = []
        with self.lock:
            ks = self.keys.setdefault(key, KeyState())
            self.paths[key] = b.get("path", "")
            off = start
            for line, ln, part in zip(lines, lens, partials):
                ks.received += 1
                rid = "%s:%d" % (key, off)
                if rid in self.seen:
                    ks.dups += 1
                    self.stats["dup_lines"] += 1
                else:
                    self.seen.add(rid)
                    ks.unique += 1
                    new_recs.append({"key": key, "offset": off, "len": ln,
                                     "line": line, "partial": part,
                                     "path": b.get("path", ""),
                                     "gen": b.get("gen", 0)})
                off += ln
        # 1) 先落盘 + fsync
        for rec in new_recs:
            self.journal.write((json.dumps(rec, ensure_ascii=False) + "\n")
                               .encode())
        os.fsync(self.journal.fileno())
        # 2) 更新高水位（在锁内重做，与写盘顺序无关）
        with self.lock:
            ks = self.keys[key]
            off = start
            for ln in lens:
                ks.add(off, off + ln)
                off += ln
            upto = ks.high_water
        # 3) 再确认 —— 故障注入点：落盘了但“确认丢失”
        ctrl = self._control()
        if ctrl.get("drop_next_ack"):
            with self.lock:
                self.stats["acks_dropped"] += 1
            try:
                atomic_save_json(self.control_path,
                                 {**ctrl, "drop_next_ack": False})
            except OSError:
                pass
            append_event(self.state_dir, "receiver", "ack_dropped",
                         key=key, upto=upto,
                         note="journaled but ack lost; collector will resend")
            return None                 # 调用方断连，不回 ack
        with self.lock:
            self.stats["acks_sent"] += 1
        return {"type": "ack", "key": key, "upto": upto, "lines": len(lines)}

    def _serve_conn(self, conn, addr):
        with self.lock:
            self.stats["connections"] += 1
        append_event(self.state_dir, "receiver", "collector_connected",
                     addr=str(addr))
        # 握手：先告知各 file_key 的已确认高水位，采集端据此直接推进 checkpoint
        with self.lock:
            hws = {k: ks.high_water for k, ks in self.keys.items()}
        try:
            send_frame(conn, {"type": "hello", "high_waters": hws})
        except OSError:
            pass
        reader = FrameReader(conn)
        try:
            while not self.stop_flag:
                try:
                    frame = reader.read_frame()
                except (OSError, ValueError):
                    break
                if frame is None:
                    break
                if frame.get("type") != "batch":
                    continue
                ctrl = self._control()
                delay = ctrl.get("delay_ms", 0)
                if delay:
                    time.sleep(delay / 1000.0)      # 模拟下游处理慢
                with self.lock:
                    self.stats["batches"] += 1
                dbg("RX key=%s start=%d n=%d", frame.get("key"),
                    frame.get("start"), len(frame.get("lines", [])))
                ack = self._handle_batch(frame)
                if ack is None:
                    break                           # ack 被丢弃：断连
                dbg("TX ack key=%s upto=%d", ack["key"], ack["upto"])
                try:
                    send_frame(conn, ack)
                except OSError:
                    break
                self._export_state()
        finally:
            try:
                conn.close()
            except OSError:
                pass
            append_event(self.state_dir, "receiver",
                         "collector_disconnected", addr=str(addr))
            self._export_state()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(4)
        srv.settimeout(0.5)
        append_event(self.state_dir, "receiver", "receiver_started",
                     pid=os.getpid(), listen="%s:%d" % (self.host, self.port))
        while not self.stop_flag:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve_conn, args=(conn, addr),
                             daemon=True).start()
        srv.close()
        self._export_state()
        append_event(self.state_dir, "receiver", "receiver_stopped",
                     pid=os.getpid())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="127.0.0.1:9099")
    ap.add_argument("--state", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--control", required=True)
    args = ap.parse_args()
    host, port = args.listen.rsplit(":", 1)
    r = Receiver((host, int(port)), args.state, args.work, args.control)

    def _term(*_):
        r.stop_flag = True
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    r.run()


if __name__ == "__main__":
    main()
