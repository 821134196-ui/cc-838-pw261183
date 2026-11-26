"""日志产生器：模拟应用写日志 + 各种轮转，同时记录 ground truth。

truth.jsonl 每行：{"seq":N, "line":"...", "partial":false}
  - partial=false：以 \n 结尾的完整行，去重后必须恰好到达下游一次
  - partial=true ：被遗弃的半行，采集器应在旧文件静默关闭时冲刷一次
半行的 truth 策略：写半行时先不记；补完 -> 记拼接后的整行；遗弃 -> 记半行。
"""
import json
import os
import time


class Producer:
    def __init__(self, logdir, state_dir, main_name="app.log"):
        self.logdir = logdir
        self.main = os.path.join(logdir, main_name)
        self.truth_path = os.path.join(state_dir, "truth.jsonl")
        self.seq = 0
        self.pending_partial = None       # 已写出但未补完的半行文本
        os.makedirs(logdir, exist_ok=True)

    def _emit_truth(self, line, partial):
        with open(self.truth_path, "a") as f:
            f.write(json.dumps({"seq": self.seq, "line": line,
                                "partial": partial}, ensure_ascii=False) + "\n")
        self.seq += 1

    def make_line(self, scenario, i):
        return "%06d|%s|%04d|%s" % (self.seq, scenario, i,
                                    "x" * (20 + self.seq % 30))

    def write_lines(self, f, scenario, n, interval=0.01):
        for i in range(n):
            line = self.make_line(scenario, i)
            f.write((line + "\n").encode())
            f.flush()
            self._emit_truth(line, False)
            if interval:
                time.sleep(interval)

    def write_partial(self, f, scenario, tag="frag"):
        assert self.pending_partial is None
        text = "%06d|%s|%s|half-line-no-newline" % (self.seq, scenario, tag)
        f.write(text.encode())
        f.flush()
        self.pending_partial = text
        return text

    def complete_partial(self, f, scenario, tag="frag"):
        """补完之前的半行：拼接后成为一整行，必须恰好到达一次。"""
        rest = "%s|...completed" % tag
        f.write((rest + "\n").encode())
        f.flush()
        self._emit_truth((self.pending_partial or "") + rest, False)
        self.pending_partial = None

    def abandon_partial(self):
        """半行永远不再补完（如旧文件被轮转走）：应被冲刷为 partial 记录。"""
        if self.pending_partial is not None:
            self._emit_truth(self.pending_partial, True)
            self.pending_partial = None


# ---------------- 轮转原语 ----------------

def rotate_rename(producer, suffix):
    """mv app.log app.log.<suffix>；调用方负责后续打开新文件。"""
    rotated = producer.main + "." + suffix
    if os.path.exists(rotated):
        os.remove(rotated)
    os.rename(producer.main, rotated)
    return rotated


def rotate_copytruncate(producer, suffix):
    """cp app.log app.log.<suffix> 然后截断 app.log（inode 不变）。
    副本先写临时名再原子改名发布——与真实 logrotate 行为一致。"""
    rotated = producer.main + "." + suffix
    # 副本先写不匹配 app.log* 的临时名，再原子改名发布
    tmp = os.path.join(os.path.dirname(producer.main), ".copying.tmp")
    with open(producer.main, "rb") as src, open(tmp, "wb") as dst:
        dst.write(src.read())
    os.rename(tmp, rotated)
    with open(producer.main, "w"):
        pass                                # truncate，inode 不变
    return rotated
