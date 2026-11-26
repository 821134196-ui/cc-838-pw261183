"""公共工具：指纹、原子写、事件日志、NDJSON 协议帧。"""
import hashlib
import json
import os
import time

FP_BYTES = 1024        # 文件指纹：前 N 字节的 sha1
TAIL_BYTES = 64        # 续传校验：acked 位置之前 N 字节的 sha1


def sha1_hex(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def short(h: str, n: int = 12) -> str:
    return h[:n] if h else h


def atomic_save_json(path: str, obj) -> None:
    """tmp + rename 原子落盘，避免崩溃时写一半的检查点。"""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_json(path: str, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def append_event(state_dir: str, src: str, kind: str, **fields) -> None:
    """事件流，dashboard 直接读。小行 O_APPEND 追加，多进程安全。"""
    rec = {"ts": round(time.time(), 3), "src": src, "kind": kind}
    rec.update(fields)
    try:
        with open(os.path.join(state_dir, "events.jsonl"), "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def read_events(state_dir: str, limit: int = 300):
    path = os.path.join(state_dir, "events.jsonl")
    try:
        with open(path) as f:
            lines = f.readlines()
    except OSError:
        return []
    out = []
    for ln in lines[-limit:]:
        try:
            out.append(json.loads(ln))
        except ValueError:
            pass
    return out


# ---------- NDJSON over TCP ----------

def send_frame(sock, obj) -> None:
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode()
    sock.sendall(data)


class FrameReader:
    """从 socket 流里切出逐行 JSON 帧。"""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def read_frame(self):
        """返回一个 dict；连接关闭返回 None。"""
        while True:
            i = self.buf.find(b"\n")
            if i >= 0:
                line, self.buf = self.buf[:i], self.buf[i + 1:]
                if line.strip():
                    return json.loads(line.decode())
                continue
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self.buf += chunk
