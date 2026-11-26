"""对账：producer 的 truth.jsonl vs 接收端 journal.jsonl。

判定：
  - 每条 truth 行必须在 journal 中出现（去重后恰好一次）-> 否则记 LOST
  - journal 中不应出现 truth 之外的行             -> 否则记 UNEXPECTED
  - 重复次数取接收端统计（重发被去重的行数）      -> 允许存在，如实报告
退出码：有 LOST/UNEXPECTED 为 1，否则 0。
"""
import json
import os
import sys


def load_lines(path):
    try:
        with open(path) as f:
            return [json.loads(ln) for ln in f if ln.strip()]
    except FileNotFoundError:
        return []


def verify(state_dir, receiver_work, verbose=True):
    truth = load_lines(os.path.join(state_dir, "truth.jsonl"))
    journal = load_lines(os.path.join(receiver_work, "journal.jsonl"))
    got = {}
    for rec in journal:
        # 完整行带尾部 \n；partial 冲刷行不带。统一去掉一个尾部 \n 再比对
        ln = rec["line"]
        if ln.endswith("\n"):
            ln = ln[:-1]
        got[ln] = got.get(ln, 0) + 1

    lost, ok = [], 0
    for t in truth:
        if t["line"] in got:
            ok += 1
        else:
            lost.append(t)
    truth_set = {t["line"] for t in truth}
    unexpected = [ln for ln in got if ln not in truth_set]
    # 同一行文本被记了多次（不同 record id）：record id 去重挡不住的幻影重复
    content_dups = sum(c - 1 for c in got.values() if c > 1)

    rcv_state = {}
    try:
        with open(os.path.join(receiver_work, "state.json")) as f:
            rcv_state = json.load(f)
    except OSError:
        pass
    dups = sum(v.get("dups", 0) for v in rcv_state.get("files", {}).values())
    unique = sum(v.get("unique", 0) for v in rcv_state.get("files", {}).values())

    report = {
        "truth_lines": len(truth),
        "truth_partials": sum(1 for t in truth if t.get("partial")),
        "journal_unique_lines": len(got),
        "receiver_unique_records": unique,
        "delivered_ok": ok,
        "lost": len(lost),
        "unexpected": len(unexpected),
        "content_duplicates": content_dups,
        "duplicates_suppressed": dups,
    }
    if verbose:
        print("=" * 64)
        print("对账结果")
        print("=" * 64)
        for k, v in report.items():
            print("  %-24s %d" % (k, v))
        if lost:
            print("\n丢失的行（前 10 条）:")
            for t in lost[:10]:
                print("  seq=%s partial=%s %r" % (t["seq"], t["partial"],
                                                  t["line"][:80]))
        if unexpected:
            print("\n多出来的行（前 10 条）:")
            for ln in unexpected[:10]:
                print("  %r" % ln[:80])
        if content_dups:
            print("\n幻影重复（同一文本多个 record id，前 10 条）:")
            for ln, c in list((k, v) for k, v in got.items() if v > 1)[:10]:
                print("  x%d %r" % (c, ln[:80]))
        verdict = "PASS: 不丢、内容级不重（传输重复已被 record id 去重）" \
            if not lost and not unexpected and not content_dups \
            else "FAIL: 有丢失或异常行"
        print("\n结论: %s" % verdict)
        print("=" * 64)
    return report, lost, unexpected


if __name__ == "__main__":
    state_dir = sys.argv[1] if len(sys.argv) > 1 else "demo_state"
    rcv_work = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        state_dir, "receiver")
    report, lost, unexpected = verify(state_dir, rcv_work)
    bad = lost or unexpected or report["content_duplicates"]
    sys.exit(1 if bad else 0)
