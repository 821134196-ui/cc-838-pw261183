"""状态页面：源文件指纹 / 已确认位置 / 下游接收情况 / 事件流。

只读 <state> 目录下各组件落盘的 JSON，自动刷新。无第三方依赖。
  python3 dashboard.py --state demo_state --port 8080
"""
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import load_json, read_events

PAGE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>尾读采集器 · 状态</title>
<style>
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface-1: #fcfcfb;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
  --gridline: #e1e0d9; --border: rgba(11,11,11,0.10);
  --good: #0ca30c; --warning: #fab219; --serious: #ec835a; --critical: #d03b3b;
  --series-1: #2a78d6; --seq-150: #b7d3f6; --seq-250: #86b6ef;
}
@media (prefers-color-scheme: dark) {
  :root {
    color-scheme: dark;
    --page: #0d0d0d; --surface-1: #1a1a19;
    --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
    --gridline: #2c2c2a; --border: rgba(255,255,255,0.10);
    --series-1: #3987e5; --seq-150: #184f95; --seq-250: #256abf;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--text-primary);
  font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
.wrap { max-width: 1180px; margin: 0 auto; padding: 20px 16px 48px; }
h1 { font-size: 18px; margin: 0 0 4px; }
h2 { font-size: 14px; margin: 0 0 10px; color: var(--text-secondary);
  font-weight: 600; }
.sub { color: var(--muted); font-size: 12px; margin-bottom: 18px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit,minmax(150px,1fr));
  gap: 10px; margin-bottom: 20px; }
.tile { background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px 14px; }
.tile .label { font-size: 12px; color: var(--muted); }
.tile .value { font-size: 26px; font-weight: 650; margin-top: 2px;
  font-variant-numeric: tabular-nums; }
.tile .hint { font-size: 11px; color: var(--text-secondary); margin-top: 2px; }
.card { background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 8px; padding: 14px 16px; margin-bottom: 18px; }
table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
th { text-align: left; color: var(--muted); font-weight: 500;
  border-bottom: 1px solid var(--gridline); padding: 4px 8px 6px 0; }
td { padding: 5px 8px 5px 0; border-bottom: 1px solid var(--gridline);
  vertical-align: middle; }
tr:last-child td { border-bottom: none; }
.num { font-variant-numeric: tabular-nums; text-align: right; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11.5px; }
.pill { display: inline-flex; align-items: center; gap: 5px; font-size: 11px;
  border: 1px solid var(--border); border-radius: 999px; padding: 1px 8px; }
.dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.meter { width: 140px; height: 8px; border-radius: 4px; overflow: hidden;
  background: var(--gridline); display: flex; }
.meter > span { display: block; height: 100%; }
.m-acked { background: var(--series-1); }
.m-sent  { background: var(--seq-250); }
.events { max-height: 320px; overflow: auto; }
.events td { border-bottom: 1px solid var(--gridline); }
.kind { font-weight: 600; }
.k-good { color: var(--good); } .k-warn { color: var(--serious); }
.k-bad  { color: var(--critical); } .k-info { color: var(--text-secondary); }
.footer { color: var(--muted); font-size: 11px; margin-top: 8px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>尾读采集器 · 运行状态</h1>
  <div class="sub" id="sub">加载中…</div>

  <div class="tiles" id="tiles"></div>

  <div class="card">
    <h2>采集端：文件指纹与已确认位置（checkpoint = acked）</h2>
    <table id="files">
      <thead><tr>
        <th>文件</th><th>file_key / 指纹</th><th>状态</th>
        <th class="num">文件大小</th><th class="num">已读 pos</th>
        <th class="num">已发 sent</th><th class="num">已确认 acked</th>
        <th class="num">滞后</th><th class="num">未确认</th><th>确认进度</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="card">
    <h2>接收端：高水位与去重（record id = file_key:offset）</h2>
    <table id="rcv">
      <thead><tr>
        <th>file_key</th><th>文件</th>
        <th class="num">高水位</th><th class="num">唯一记录</th>
        <th class="num">累计收到</th><th class="num">重复被去重</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="card">
    <h2>事件流（轮转 / 截断 / 重启 / 确认丢失）</h2>
    <div class="events">
      <table id="events">
        <thead><tr><th>时间</th><th>来源</th><th>事件</th><th>详情</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="footer">每 1.5s 自动刷新 · 数据来自各组件落盘的 JSON，页面本身无状态</div>
  </div>
</div>
<script>
const fmt = n => n === null || n === undefined ? "—" : Number(n).toLocaleString();
const esc = s => String(s).replace(/[&<>"]/g, c =>
  ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const KIND_CLASS = {
  copytruncate_detected:"k-warn", copytruncate_adopted:"k-warn",
  rewrite_detected:"k-warn", data_gap:"k-bad", ack_dropped:"k-bad",
  file_gone:"k-warn", partial_flushed:"k-warn",
  rename_detected:"k-info", new_generation:"k-info",
  file_discovered:"k-info", downstream_connected:"k-good",
  downstream_disconnected:"k-warn", collector_started:"k-good",
  checkpoint_loaded:"k-warn", journal_rebuilt:"k-good",
  file_closed_idle:"k-info", file_ended:"k-info",
};
function pill(text, color) {
  return `<span class="pill"><span class="dot" style="background:${color}"></span>${text}</span>`;
}
function statePill(f) {
  if (f.ended) return pill("已关闭", "var(--muted)");
  if (f.gone)  return pill("已轮转走(fd续读)", "var(--serious)");
  if (f.unacked > 0) return pill("传输中", "var(--series-1)");
  return pill("已确认", "var(--good)");
}
function meter(f) {
  const size = f.size || Math.max(f.pos, 1);
  const pa = Math.min(100, 100 * f.acked / size);
  const ps = Math.min(100 - pa, 100 * Math.max(0, f.sent - f.acked) / size);
  return `<div class="meter" title="acked ${fmt(f.acked)} / sent ${fmt(f.sent)} / size ${fmt(size)}">
    <span class="m-acked" style="width:${pa}%"></span>
    <span class="m-sent" style="width:${ps}%"></span></div>`;
}
async function refresh() {
  let d;
  try { d = await (await fetch("/state.json")).json(); }
  catch (e) { return; }
  const c = d.collector || {}, r = d.receiver || {};
  const cs = c.stats || {}, rs = r.stats || {};
  document.getElementById("sub").textContent =
    `采集端${c.connected ? "已连接" : "未连接"}下游 · 在途批次 ${fmt(c.inflight_batches)} · ` +
    `重启 ${fmt(cs.restarts)} 次 · 重连 ${fmt(cs.reconnects)} 次 · ` +
    `更新于 ${d.ts ? new Date(d.ts*1000).toLocaleTimeString() : "—"}`;
  const tiles = [
    ["已发送行", cs.lines_sent, "采集端累计"],
    ["已确认行", cs.lines_acked, "收到下游 ack"],
    ["下游唯一记录", r.unique_total, "去重后落盘"],
    ["重复被去重", rs.dup_lines, "重发被 record id 拦截"],
    ["ack 丢失", rs.acks_dropped, "已落盘但确认未回（故障注入）"],
    ["滞后字节", d.lag_total, "文件大小 − 已发"],
  ];
  document.getElementById("tiles").innerHTML = tiles.map(([l,v,h]) =>
    `<div class="tile"><div class="label">${l}</div>
     <div class="value">${fmt(v)}</div><div class="hint">${h}</div></div>`).join("");
  document.querySelector("#files tbody").innerHTML = (c.files || []).map(f =>
    `<tr><td class="mono">${esc(f.path.split("/").pop())}${f.gen ? " · g"+f.gen : ""}</td>
     <td class="mono" title="fingerprint ${esc(f.fingerprint)}">${esc(f.key)}<br>
       <span style="color:var(--muted)">fp ${esc((f.fingerprint||"").slice(0,12))}…</span></td>
     <td>${statePill(f)}</td>
     <td class="num">${fmt(f.size)}</td><td class="num">${fmt(f.pos)}</td>
     <td class="num">${fmt(f.sent)}</td><td class="num"><b>${fmt(f.acked)}</b></td>
     <td class="num">${fmt(f.lag)}</td><td class="num">${fmt(f.unacked)}</td>
     <td>${meter(f)}</td></tr>`).join("");
  const rf = r.files || {};
  document.querySelector("#rcv tbody").innerHTML = Object.entries(rf).map(([k, v]) =>
    `<tr><td class="mono">${esc(k)}</td>
     <td class="mono">${esc((v.path||"").split("/").pop())}</td>
     <td class="num">${fmt(v.high_water)}</td><td class="num">${fmt(v.unique)}</td>
     <td class="num">${fmt(v.received)}</td>
     <td class="num">${v.dups ? '<b style="color:var(--serious)">'+fmt(v.dups)+"</b>" : "0"}</td></tr>`
  ).join("");
  document.querySelector("#events tbody").innerHTML =
    (d.events || []).slice().reverse().map(e =>
    `<tr><td class="num mono">${new Date(e.ts*1000).toLocaleTimeString()}</td>
     <td>${esc(e.src)}</td>
     <td class="kind ${KIND_CLASS[e.kind]||""}">${esc(e.kind)}</td>
     <td class="mono" style="color:var(--text-secondary)">${esc(
        Object.entries(e).filter(([k]) => !["ts","src","kind"].includes(k))
              .map(([k,v]) => k+"="+v).join(" ").slice(0,140))}</td></tr>`).join("");
}
refresh();
setInterval(refresh, 1500);
</script>
</body>
</html>
"""


def build_state(state_dir):
    collector = load_json(os.path.join(state_dir, "collector", "status.json"), {})
    receiver = load_json(os.path.join(state_dir, "receiver", "state.json"), {})
    lag_total = sum(f.get("lag") or 0 for f in collector.get("files", []))
    unique_total = sum(v.get("unique", 0)
                       for v in receiver.get("files", {}).values())
    return {"ts": max(collector.get("ts", 0), receiver.get("ts", 0)),
            "collector": collector, "receiver": receiver,
            "lag_total": lag_total, "unique_total": unique_total,
            "events": read_events(state_dir, limit=200)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()
    state_dir = os.path.abspath(args.state)

    class Handler(BaseHTTPRequestHandler):
        def _send(self, body, ctype):
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.startswith("/state.json"):
                self._send(json.dumps(build_state(state_dir),
                                      ensure_ascii=False),
                           "application/json")
            else:
                self._send(PAGE, "text/html; charset=utf-8")

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print("dashboard: http://127.0.0.1:%d/  (state=%s)" % (args.port, state_dir))
    srv.serve_forever()


if __name__ == "__main__":
    main()
