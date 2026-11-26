"""端到端编排：真实子进程跑接收端/采集器，按剧本制造故障，最后对账。

剧本：
  1. baseline          正常追加
  2. partial-complete  半行挂起 1.5s 后补完 -> 应只出一条完整行
  3. rename            rename 后旧 fd 继续写 30 行（旧文件还在写）
  4. partial-rename    半行跨 rename：旧 fd 上补完
  5. abandoned         半行随旧文件被遗弃 -> 静默关闭时冲刷为 partial
  6. copytruncate      cp + truncate，副本认领，新一代从 0 读
  7. lag + kill -9     下游变慢 -> 背压滞后 -> 杀掉采集器 -> 重启续传
  8. lost-ack          下游落盘但丢 ack -> 重发 -> 接收端去重
  9. final             收尾，等全部确认，对账
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from common import load_json  # noqa: E402
from producer import Producer, rotate_copytruncate, rotate_rename  # noqa: E402
from verify import verify  # noqa: E402


class Demo:
    def __init__(self, root, recv_port=9099, dash_port=8080):
        self.root = os.path.abspath(root)
        self.state = os.path.join(self.root)          # 共享状态目录
        self.logs = os.path.join(self.root, "logs")
        self.col_work = os.path.join(self.root, "collector")
        self.rcv_work = os.path.join(self.root, "receiver")
        self.control = os.path.join(self.root, "control.json")
        self.recv_port = recv_port
        self.dash_port = dash_port
        self.procs = {}
        self.ok = True

    # ---------- 进程管理 ----------

    def _spawn(self, name, argv):
        logf = open(os.path.join(self.root, name + ".log"), "ab")
        p = subprocess.Popen([sys.executable] + argv,
                             cwd=HERE, stdout=logf, stderr=subprocess.STDOUT)
        self.procs[name] = p
        return p

    def start_receiver(self):
        return self._spawn("receiver", [
            "receiver.py", "--listen", "127.0.0.1:%d" % self.recv_port,
            "--state", self.state, "--work", self.rcv_work,
            "--control", self.control])

    def start_collector(self):
        return self._spawn("collector", [
            "collector.py", "--dir", self.logs, "--pattern", "app.log*",
            "--state", self.state, "--work", self.col_work,
            "--receiver", "127.0.0.1:%d" % self.recv_port])

    def start_dashboard(self):
        return self._spawn("dashboard", [
            "dashboard.py", "--state", self.state, "--port",
            str(self.dash_port)])

    def kill_collector(self):
        p = self.procs.get("collector")
        if p and p.poll() is None:
            p.send_signal(signal.SIGKILL)       # 模拟崩溃：无任何清理
            p.wait()

    def stop(self, name, sig=signal.SIGTERM, timeout=8):
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.send_signal(sig)
            try:
                p.wait(timeout)
            except subprocess.TimeoutExpired:
                p.kill()

    # ---------- 工具 ----------

    def set_control(self, **kw):
        cur = load_json(self.control, {}) or {}
        cur.update(kw)
        with open(self.control, "w") as f:
            json.dump(cur, f)

    def col_status(self):
        return load_json(os.path.join(self.col_work, "status.json"), {})

    def rcv_state(self):
        return load_json(os.path.join(self.rcv_work, "state.json"), {})

    def truth_count(self):
        try:
            with open(os.path.join(self.state, "truth.jsonl")) as f:
                return sum(1 for _ in f)
        except OSError:
            return 0

    def wait_until(self, desc, cond, timeout=40, debug=None):
        t0 = time.time()
        last_dbg = None
        while time.time() - t0 < timeout:
            if cond():
                print("  [ok] %s (%.1fs)" % (desc, time.time() - t0))
                return True
            if debug:
                dbg = debug()
                if dbg != last_dbg:
                    print("  ... %s: %s" % (desc, dbg))
                    last_dbg = dbg
            time.sleep(0.2)
        print("  [TIMEOUT] %s" % desc)
        self.ok = False
        return False

    def all_acked(self):
        st = self.col_status()
        files = st.get("files", [])
        return bool(files) and all(
            f["sent"] == f["acked"] and not f["pending"] for f in files)

    def settled(self, prod):
        """生产侧已写的行全部到达接收端且采集端无在途/半行。"""
        return self.rcv_unique() >= prod.seq and self.all_acked()

    def rcv_unique(self):
        return sum(v.get("unique", 0)
                   for v in self.rcv_state().get("files", {}).values())

    def rcv_dups(self):
        return sum(v.get("dups", 0)
                   for v in self.rcv_state().get("files", {}).values())

    # ---------- 剧本 ----------

    def run(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root)
        for d in (self.state, self.logs, self.col_work, self.rcv_work):
            os.makedirs(d)
        self.set_control(delay_ms=0, drop_next_ack=False)

        self.start_receiver()
        self.start_dashboard()
        self.start_collector()
        time.sleep(1.0)
        prod = Producer(self.logs, self.state)
        f = open(prod.main, "ab")

        print("== 1. baseline：正常追加 100 行")
        prod.write_lines(f, "baseline", 100, interval=0.01)
        self.wait_until("baseline 全部确认",
                        lambda: self.rcv_unique() >= 100)

        print("== 2. partial-complete：半行挂起 1.5s 后补完")
        prod.write_partial(f, "partial-complete")
        time.sleep(1.5)
        prod.complete_partial(f, "partial-complete")
        self.wait_until("补完行被确认", lambda: self.settled(prod))

        print("== 3. rename：rename 后旧 fd 继续写 30 行")
        prod.write_lines(f, "rename-before", 50, interval=0.005)
        rotate_rename(prod, "1")
        prod.write_lines(f, "rename-old-tail", 30, interval=0.005)  # 进 app.log.1
        f.close()
        f = open(prod.main, "ab")                                    # 新 app.log
        prod.write_lines(f, "rename-new-head", 20, interval=0.005)

        print("== 4. partial-rename：半行跨 rename 补完")
        prod.write_partial(f, "partial-rename")
        rotate_rename(prod, "2")
        time.sleep(0.5)
        prod.complete_partial(f, "partial-rename")                   # 写进 app.log.2
        prod.write_lines(f, "rename2-old-tail", 10, interval=0.005)
        f.close()
        f = open(prod.main, "ab")

        print("== 5. abandoned：半行随旧文件被遗弃，等静默关闭冲刷")
        prod.write_partial(f, "abandoned")
        rotate_rename(prod, "3")
        f.close()                        # 应用关掉旧 fd，半行永远留在 app.log.3
        prod.abandon_partial()
        f = open(prod.main, "ab")
        self.wait_until("遗弃半行被冲刷确认", lambda: self.settled(prod),
                        timeout=20)

        print("== 6. copytruncate：cp + truncate，副本认领")
        prod.write_lines(f, "ct-before", 50, interval=0.005)
        time.sleep(0.5)
        rotate_copytruncate(prod, "4")
        prod.write_lines(f, "ct-after", 40, interval=0.005)  # O_APPEND 写到新起点
        self.wait_until("copytruncate 后全部确认", lambda: self.settled(prod))

        print("== 7. lag + kill -9：下游变慢 -> 滞后 -> 杀采集器 -> 重启")
        self.set_control(delay_ms=150)
        dups_before = self.rcv_dups()
        prod.write_lines(f, "lag", 400, interval=0.002)
        # 等背压真正形成再杀，保证"杀掉时有未确认数据"一定成立
        self.wait_until("背压形成(在途批次>0)",
                        lambda: (self.col_status().get("inflight_batches")
                                 or 0) > 0, timeout=15)
        time.sleep(0.3)
        st = self.col_status()
        lag_seen = sum(x.get("lag") or 0 for x in st.get("files", []))
        print("  杀掉前: 未确认积压 %d 字节, 在途批次 %s"
              % (lag_seen, st.get("inflight_batches")))
        self.kill_collector()
        print("  采集器已被 kill -9，1s 后重启")
        time.sleep(1.0)
        self.start_collector()
        self.set_control(delay_ms=0)
        def _dbg():
            fs = [(f["key"].split(":")[-1], f["sent"], f["acked"], f["pending"])
                  for f in self.col_status().get("files", [])]
            return "unique=%d/%d all_acked=%s files=%s" % (
                self.rcv_unique(), prod.seq, self.all_acked(), fs)
        self.wait_until("重启后追平并全部确认", lambda: self.settled(prod),
                        timeout=60, debug=_dbg)
        dups_after = self.rcv_dups()
        print("  重启导致的重复（已被接收端去重）: %d 行"
              % (dups_after - dups_before))

        print("== 8. lost-ack：下游落盘但丢 ack -> 重发 -> 去重")
        dups_before = self.rcv_dups()
        self.set_control(drop_next_ack=True)
        prod.write_lines(f, "lost-ack", 30, interval=0.01)
        self.wait_until("ack 丢失后重发并确认",
                        lambda: self.settled(prod) and
                        self.rcv_dups() > dups_before, timeout=30)
        print("  ack 丢失导致的重复（已被去重）: %d 行"
              % (self.rcv_dups() - dups_before))

        print("== 9. final：收尾")
        prod.write_lines(f, "final", 30, interval=0.005)
        f.close()
        self.wait_until("全部行到达且确认", lambda: self.settled(prod),
                        timeout=30)

        print("\n== 收尾：停采集器/接收端，对账")
        self.stop("collector")
        self.stop("receiver")
        report, lost, unexpected = verify(self.state, self.rcv_work)
        ck = load_json(os.path.join(self.col_work, "checkpoint.json"), {})
        print("\ncheckpoint 文件留存 %d 个 file_key：" % len(ck.get("files", {})))
        for k, v in sorted(ck.get("files", {}).items()):
            print("  %-18s acked=%-7d fp=%s… %s" % (
                k, v["acked"], (v.get("fingerprint") or "")[:12],
                os.path.basename(v["path"])))
        bad = lost or unexpected or report["content_duplicates"]
        return self.ok and not bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/tmp/tail-collector-demo")
    ap.add_argument("--serve", action="store_true",
                    help="结束后保持 dashboard 运行")
    ap.add_argument("--dash-port", type=int, default=8080)
    ap.add_argument("--recv-port", type=int, default=9099)
    args = ap.parse_args()
    demo = Demo(args.root, recv_port=args.recv_port, dash_port=args.dash_port)
    ok = demo.run()
    print("\n总体结论: %s" % ("PASS" if ok else "FAIL"))
    if args.serve:
        print("dashboard 仍在运行: http://127.0.0.1:%d/  (Ctrl-C 结束)"
              % args.dash_port)
        try:
            demo.procs["dashboard"].wait()
        except KeyboardInterrupt:
            demo.stop("dashboard")
    else:
        demo.stop("dashboard")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
