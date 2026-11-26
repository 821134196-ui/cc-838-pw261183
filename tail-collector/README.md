# 尾读采集器（tail collector）

一个演示**至少一次（at-least-once）日志采集语义**的最小系统：尾读 `app.log*`，
处理 rename 后旧文件继续写、copytruncate、半行（最后一行没有 `\n`），
把进度 checkpoint 在本地，模拟下游变慢、进程崩溃、ack 丢失，
最后用 ground truth 对账验证**不丢、内容级不重**。

```
producer(模拟应用) --写日志/轮转--> logs/app.log*
                                        | 尾读
                                   collector --NDJSON/TCP--> receiver
                                        |                        |
                                  checkpoint.json          journal.jsonl
                                        \________ dashboard ________/
                                            http://127.0.0.1:8080/
```

## 运行

```bash
python3 run_demo.py                 # 跑完整故障剧本并对账（默认状态目录 /tmp/tail-collector-demo）
python3 run_demo.py --serve         # 跑完后保持 dashboard 可访问
```

剧本共 9 幕：baseline → 半行补完 → rename 后旧 fd 继续写 → 半行跨 rename →
半行被遗弃 → copytruncate → 下游变慢+`kill -9` 采集器+重启 → 下游落盘但丢 ack → 收尾对账。
退出码 0 = 不丢不重（传输重复已被去重）。

> 状态目录请放在普通文件系统上（默认 `/tmp`）。`/workspace` 是 9p 挂载，
> 会触发额外的时序怪异（见文末「网络文件系统的教训」），代码已针对它加固，
> 但演示在普通文件系统上更稳定。

## 检查点（checkpoint）和确认（ack）怎么配合

每个文件“代”（generation，见下）在采集器内存里维护**三个位置**：

| 位置   | 含义                     | 持久化？          |
|--------|--------------------------|-------------------|
| `pos`  | 读到了哪里               | 否                |
| `sent` | 发到了哪里（行边界）     | 否                |
| `acked`| 下游确认到了哪里         | **是（checkpoint）** |

配合规则只有两条：

1. **接收端先落盘、后确认**：收到批次 → 新记录追加进 `journal.jsonl` 并 `fsync`
   → 才回 ack。因此“已确认”必然“已持久化”，ack 不会撒谎。
2. **采集端只把 acked 写进 checkpoint**：`checkpoint.json` 里每个 file_key 只存
   `acked`（外加指纹等元数据）。`pos`/`sent` 是易失的。

于是任何时刻崩溃、重启、断连，恢复动作都是同一个：**从 acked 重新读、重新发**。
`[acked, sent)` 这段“已发未确认”的数据会被重发——这是刻意的，因为采集端
无法区分“下游没收到”和“下游收到了但 ack 丢了”。

连接层面还有两个配合机制：

- **累计高水位 ack**：ack 携带 `upto=高水位`（从 0 起连续覆盖到的偏移），
  一个 ack 确认此前全部数据，个别 ack 丢失可被后续 ack 补偿。
- **hello 握手**：每次（重）连，接收端先告知各 file_key 的已持久化高水位，
  采集端直接推进 `acked`——上次崩溃前“已落盘但 ack 没到”的数据不必重发
  （本 demo 中 kill -9 后的重复因此只有百行量级而非全部）。

背压：在途未确认批次超过 `MAX_INFLIGHT_BATCHES=8` 时采集器停止读取，
滞后以“未确认积压字节”暴露在 dashboard 上。

## 为什么不能同时承诺“不丢”和“不重”

这是两将军问题的直接推论：采集端发出数据后没等到 ack，**无法区分**
“下游没收到”与“下游收到了但确认丢了”。要保证不丢，唯一选择是重发；
重发就可能重复。所以端到端 exactly-once 在不可靠信道上不可承诺，
工程上的答案是：

> **采集端 at-least-once（不丢） + 接收端幂等去重（效果上不重复）。**

## 接收端可验证的去重办法

**记录 id = `{file_key}:{byte_offset}`**，其中
`file_key = {dev}:{inode}:g{代}`。同一行在源文件里的位置是确定的，
重发多少次 record id 都不变，接收端用它去重：

- 接收端持久化 `journal.jsonl`（每条新记录一行，先 fsync 再 ack）。
  重启后重放 journal 重建去重集合——已确认的永远不会因接收端重启而重复入库。
- 每个 file_key 维护**区间集合 + 高水位**：区间用于精确判重，
  高水位（从 0 起连续覆盖）用于累计 ack。
- **可验证/可审计**：journal 是只追加的事实日志，任何人可以重放它，
  核对“每条 record id 恰好入库一次”“高水位连续无洞”。
  `verify.py` 就是这么对账的：producer 的 `truth.jsonl` vs 接收端 journal，
  报告丢失、意外行、**内容级重复**（同一文本出现在不同 record id 下——
  那才说明采集端判代错误产生了幻影重复，传输重发不算）。

判重完全在接收端闭环，不依赖采集端守规矩：即使采集端 bug 重发了整段文件，
record id 依然稳定，接收端照样去重。

## 三种轮转场景的处理

- **rename 后旧文件还在写**：fd 一旦打开就钉在 inode 上，rename 只更新路径，
  绝不按路径重开。旧文件（如 `app.log.1`）继续读到静默超 `ROTATE_GRACE=2s`
  才关闭。关闭前把残余半行作为 `partial:true` 记录冲刷出去。
- **copytruncate**：检测到“截断”（见下）后，在目录里找**指纹匹配的未跟踪
  文件**（logrotate 先 `cp` 再截断，副本头部指纹与原文件一致）→ 把状态
  （file_key、三个位置）迁移到副本上继续读，原 inode 开启新一代 `g{n+1}`
  从 0 读。找不到副本则记 `data_gap` 事件（copytruncate 本身有固有丢失窗口：
  cp 完成到 truncate 之间写入的数据物理上无法挽回，只能检测并上报）。
- **半行**：没有 `\n` 的尾部字节留在 `pending` 缓冲区（不变式：
  `pending == 文件[sent:pos]`），等到补完再作为完整行发出；
  只有文件被轮转走且静默超时，才作为 `partial:true` 冲刷。
  半行跨 rename 在旧 fd 上补完的场景也能正确拼回一整行。

**代（generation）**：同一 inode 被截断/重写后是一个新代，`g` 递增，
record id 随之变化——这是必要的：截断后偏移 0 的字节已经不是原来那批了。

## 故障注入与复现

`control.json`（运行时可改）：

- `{"delay_ms": 150}`：接收端每批处理前睡眠 → 采集端背压，dashboard 可见滞后。
- `{"drop_next_ack": true}`：接收端**落盘后不回 ack 直接断连**——
  复现“下游收到了但确认丢失”。采集端重连后从 acked 重发，
  接收端按 record id 去重，dashboard 的“重复被去重”计数器 +N。

`run_demo.py` 第 7 幕在背压形成后 `kill -9` 采集器再重启，演示崩溃恢复；
第 8 幕用 `drop_next_ack` 复现丢 ack。两幕产生的重复行数都会打印并被对账确认。

## Dashboard

`dashboard.py`（stdlib http.server，端口 8080，1.5s 自动刷新）：

- 指标瓦片：已发送行 / 已确认行 / 下游唯一记录 / 重复被去重 / ack 丢失次数 / 滞后字节
- 采集端文件表：路径、file_key、**源文件指纹**、状态、size/pos/sent/**acked**、
  滞后、未确认字节、确认进度条
- 接收端表：每个 file_key 的高水位、唯一记录数、收到总数、去重数
- 事件流：轮转、认领、断连、ack 丢失、半行冲刷等

## 网络文件系统的教训（9p/drvfs 加固，同样适用于 NFS）

本系统最初在 9p 挂载（WSL2 的 `/workspace`）上联调，暴露出三类真实问题，
对应的防御对生产环境（NFS 等）同样有效：

1. **stat 不可信**：9p 的属性和目录项缓存会返回过期甚至张冠李戴的
   `st_size`，据此判断截断/重写会产生幻影。→ 一律走**内容通道**：
   怀疑截断时 `pread` 声明尺寸处必须返回 EOF 才确认；
   怀疑重写时 `pread` 头部与指纹比对；另加 0.5s/1.0s 去抖。
2. **按路径 open 会开错文件**：目录项缓存里名字和 inode 的对应关系可能已过期。
   → `open()` 后立刻 `fstat` 校验 (dev, inode) 是不是预期的那个，
   不是就当打开失败下轮重试（**打开即验明正身**）。
3. **写读缓存不连通**：极端情况下生产者写入的数据 ~60s 内对另一进程的
   `pread` 不可见。采集器对此天然正确（读不到就等，可见后再发），
   但演示脚本要放在普通文件系统上跑才稳定。

另外：`dbg` 调试输出由环境变量 `TC_DEBUG=<前缀>` 开启，分别写
`<前缀>.collector` / `<前缀>.receiver`，用于回放收发时序。

## 文件

| 文件 | 作用 |
|------|------|
| `collector.py` | 采集器：发现/跟踪文件、轮转处理、背压、checkpoint |
| `receiver.py`  | 接收端：journal 先落盘后 ack、record id 去重、故障注入 |
| `producer.py`  | 模拟应用：写日志、半行、rename/copytruncate，记录 ground truth |
| `run_demo.py`  | 端到端剧本编排（9 幕）+ 对账入口 |
| `verify.py`    | truth vs journal 对账，报告丢失/幻影重复/被去重的传输重复 |
| `dashboard.py` | 状态页面（指纹、已确认位置、下游接收情况） |
| `common.py`    | NDJSON 帧、原子写 JSON、事件流等公共件 |
