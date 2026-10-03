# 本地行情接入与 K 线聚合服务

在 Spring Boot 3 / Java 21 骨架上实现的**本地、确定性**行情接入与固定窗口 K 线聚合服务。
用本地注入的逐笔成交模拟数据源，使**乱序、重复、缺口**场景下的聚合结果确定、可解释、可复现。

- 多标的逐笔行情接入，重复 / 时间戳倒退 / 乱序按明确规则接受或拒绝，结果可区分
- 固定时间窗口聚合 OHLCV（开高低收、成交量），窗口归属与边界唯一确定
- 无行情窗口缺口标记；迟到数据可回补修正且不重复累计
- 基于签名游标的增量拉取：重复拉取不重不漏，过期 / 非法 / 串标游标明确拒绝
- 事件流可独立重建视图：从任意有效游标按序重放，逐窗口结果与区间 K 线查询完全一致
- 窗口结算语义：被更晚窗口推进且超过乱序容忍后明确 SETTLED，结算后不可再修正
- 已结算历史可压缩 / 归档：归档保留时旧游标照常回放；物理清除后明确判 UNAVAILABLE 并给出恢复起点
- 每标的独立加锁，并发接入与并发查询下同一窗口结果一致

---

## 1. 核心规则（唯一确定）

### 1.1 窗口归属与边界

- 窗口大小 `W = market-data.window-size-millis`（默认 1000ms）。
- 窗口**左闭右开**：

  ```
  start = ts - floorMod(ts, W)          // 等价于 floor(ts/W)*W
  窗口 = [start, start + W)
  ```

  使用 `Math.floorDiv/floorMod`，对负时间戳同样得到“不晚于 ts 的最大 W 整数倍”，无歧义。
- 恰好落在 `start + W` 整点的成交归入**下一**窗口（见聚合测试 `windowBoundaryIsLeftClosedRightOpen`）。

### 1.2 OHLCV 聚合

- open：窗口内**时间戳最早**的成交价；多笔共享最早时间戳时以**先接受者**为准。
- close：窗口内**时间戳最晚**的成交价；多笔共享最晚时间戳时以**后接受者**为准（“最新成交定价”）。
- high / low：窗口内全部成交价的最大 / 最小值。
- volume：窗口内全部被接受成交的成交量之和。

因此窗口内的乱序到达只按时间戳归位，结果与到达顺序无关；时间戳完全相同时，
按“同一标的接入串行化”的接受顺序打破平局，规则可解释。

### 1.3 接入判定（互斥、可区分）

判定优先级自上而下：

| 状态 | 触发条件 | 是否改变数据 |
| --- | --- | --- |
| `DUPLICATE` | `(symbol, tradeId)` 已接受过；**判重优先于倒退判定**，重放必须幂等且可识别 | 否 |
| `REJECTED_INVALID` | symbol 空白、tradeId/timestamp/volume 非正、price 非正 / NaN / 无穷 | 否（不进入存储层） |
| `REJECTED_STALE` | 时间戳早于当前水位，且倒退量 `watermark - ts` **严格大于** `out-of-order-tolerance-millis` | 否 |
| `ACCEPTED_LATE` | 时间戳早于水位但倒退量 `<=` 容忍值；按时间戳归位聚合，水位不变 | 是（修正） |
| `ACCEPTED` | 首笔，或时间戳不早于水位；正常推进水位 | 是 |

- 倒退容忍边界为闭区间：倒退恰好等于容忍值时 `ACCEPTED_LATE`，多 1ms 才 `REJECTED_STALE`。
- 每次判定都打印日志：输入行情（`[INGEST] tick=...`）与判定依据（`判定依据: ...`）。

### 1.4 缺口标记与回补

- 当被接受行情把水位从窗口 `A` 推进到窗口 `B`（`B > A`）时，
  严格位于中间的窗口 `(A, B)`（步长 W）若此前不存在，一律标记为缺口：
  `gap=true`、OHLC 为 null、`volume=0`、`version=1`，并下发 `GAP_MARKED` 事件。
- 缺口窗口收到容忍范围内的迟到成交时：缺口转为正式 K 线，`corrected=true`、
  `version +1`，并下发 `CORRECTION` 事件（携带完整新快照）。
- 已聚合（非缺口）窗口收到迟到成交时：OHLCV 按时间戳重算，同样 `corrected=true`、
  `version +1`、下发 `CORRECTION`。
- 回补的幂等：回补成交本身也走 `(symbol, tradeId)` 判重，重复回补判 `DUPLICATE`，
  成交量不重复累计、版本不变。
- 早于“最早已观测窗口”的迟到成交：直接为该更早窗口开窗（不反推更早的缺口），
  下发 `WINDOW_OPEN`，水位不变。
- 超过容忍的迟到回补判 `REJECTED_STALE`，缺口保持缺口，不被污染。

### 1.5 窗口结算（唯一确定、可复现）

一个窗口 `w`（区间 `[w, w+W)`）**同时**满足以下两个条件时结算，并为其追加一条
`SETTLED` 事件（携带最终完整快照，`settled=true`）：

1. **已被更晚窗口推进**：`w < maxWindowStart`（当前水位所在的开放窗口永不结算）；
2. **已超过乱序容忍**：`watermark >= w + W + tolerance`。

判定依据与 1.3 的迟到规则严格一致、互为对偶：

- 条件 2 成立时，任何还会被接受的行情时间戳都满足 `ts >= watermark - tolerance >= w + W`，
  即不可能再落入窗口 `w`（窗口右端点为 `w+W`，左闭右开）。因此**结算之后不可能再有修正**：
  能影响 `w` 的成交必然迟到量 `> tolerance`，一律 `REJECTED_STALE`，缺口保持缺口、
  K 线保持最终 OHLCV，版本不再增长。
- 反之只要 `watermark < w + W + tolerance`，`w` 仍可能收到容忍内的迟到成交，
  **绝不提前结算**；回补 / 修正照常产生 `CORRECTION`，待截止线到达后再结算。
- 截止线是闭区间：`watermark` 恰好等于 `w + W + tolerance` 时立即结算，
  与“倒退恰好等于容忍时仍接受（`ACCEPTED_LATE`）”的闭区间口径一致，无 1ms 歧义。

客户端可依据事件区分一个窗口的四种状态，决定能否安全落库：

| 状态 | 事件依据 | 含义 |
| --- | --- | --- |
| 刚出现 | 首条 `WINDOW_OPEN`（或缺口场景下的 `GAP_MARKED`） | 窗口进入视图，后续仍可能变化 |
| 缺口 | `GAP_MARKED`（`gap=true`） | 跨水位发现的无行情窗口，仍可能被回补 |
| 被修正 | `CORRECTION`（`corrected=true`、`version+1`） | 容忍内迟到数据重算了 OHLCV / 回补了缺口 |
| 已结算 | `SETTLED`（`settled=true`） | 结论最终确定，可安全落库，服务端不会再下发该窗口事件 |

说明：窗口未结算期间，正序进入**当前开放窗口**的行情会持续下发 `WINDOW_UPDATE`
（见 1.6），其快照也可能变化；只有收到 `SETTLED` 才是“不会再变”的唯一信号。

### 1.6 增量游标与事件重放（不重不漏、可独立重建视图）

- 每个标的维护**只追加**的事件日志，`seq` 从 1 单调递增，事件类型：
  `WINDOW_OPEN`（窗口首次产生数据）、`WINDOW_UPDATE`（开放窗口内正序行情更新）、
  `GAP_MARKED`（缺口标记）、`CORRECTION`（迟到数据修正窗口，含缺口回补）、
  `SETTLED`（窗口结算，最终快照）。每条事件都携带对应窗口的**完整快照**。
- **事件流独立重建视图**：客户端从任意有效游标开始，按 seq 顺序应用事件，
  对每个窗口只保留“该窗口最后一条事件携带的快照”，即可重建出与
  `GET .../candles` 区间查询**逐字段一致**（OHLCV、volume、gap、corrected、
  version、settled）的结果。因此：
  - 开放窗口内的实时更新也必须可见：正序并入当前窗口的每笔成交都追加 `WINDOW_UPDATE`
    （早期版本开放窗口不产生事件，会导致“事件里是一套、快照里是另一套”，现已对齐）；
  - 缺口回补、已聚合窗口修正都通过 `CORRECTION` 下发新完整快照，旧快照被覆盖；
  - `SETTLED` 是窗口最后一条事件，折叠后即得到不可再变的最终结论；
  - 行情继续进来、窗口被更晚窗口推进时，该窗口若有新状态（修正/结算）一定会有
    对应新事件，事件侧与快照侧始终是同一份数据。
- 游标为不透明签名串：`base64url(symbol|seq|issuedAt) + "." + base64url(HMAC-SHA256(payload))`。
  游标中的 `seq` 表示“已交付到该序号”，服务端始终返回 `event.seq > cursor.seq` 的事件，
  因此**同一游标重放多次结果完全一致**（不同分页大小也一致），不重不漏。
  从“任意有效游标”开始的重放保证：凡是在该游标之后交付过最新状态的窗口，
  折叠结果与当前区间快照一致；游标之前的窗口（之后再无事件）不在本次增量视图内。
- 非法游标按原因区分拒绝：

  | reason | 条件 | HTTP |
  | --- | --- | --- |
  | `MALFORMED` | 非两段式、base64 不可解码、载荷字段数/数字非法、seq 为负 | 400 |
  | `BAD_SIGNATURE` | HMAC 校验失败（被篡改 / 密钥不符） | 400 |
  | `SYMBOL_MISMATCH` | 游标属于其他标的 | 400 |
  | `EXPIRED` | 超过 `cursor-ttl-millis` | 410 Gone |
  | `UNAVAILABLE` | 游标位置已被压缩并物理清除（见 1.7） | 410 Gone |

### 1.7 事件流压缩 / 归档与游标兼容范围

长时间运行时，已结算窗口的事件允许从**热事件日志**移出，控制事件流体积。
压缩单位是**连续 seq 前缀**：从 seq=1 起顺序扫描，只要事件所属窗口已结算就纳入归档，
遇到第一个“所属窗口未结算”的事件即停止（窗口按起点有序，未结算窗口之后不可能出现
可归档事件）。因此归档的总是 `1..k` 的连续前缀，**seq 全局保持不变、不复用**，
归档段 + 热段在逻辑上仍是同一条只追加序列。

注意：一个窗口的事件**可能跨在边界两侧**。例如 `w0` 的开窗事件很早、它的
`SETTLED` 却要等水位越过截止线才追加（序号位于更晚窗口的开窗 / 缺口事件之后），
于是 `w0` 的 `OPEN` 被归档而 `SETTLED` 仍在热段。这不影响正确性：拉取时按全局
seq 顺序合并归档段与热段，且每条事件都携带**完整快照**，客户端折叠到的仍是该窗口
的最终状态；物理清除后用恢复游标继续，也不会丢掉窗口的 `SETTLED`。

两种模式（通过管理接口触发，见第 4 节）：

| 模式 | 行为 | 旧游标能否继续 |
| --- | --- | --- |
| `archive`（默认） | 已结算前缀移入归档，归档保留可回放 | **完全兼容**：任意早期游标继续拉取，事件序列与压缩前逐事件一致（不漏不重） |
| `trim` | 先归档再**物理清除**归档 | `seq < hotBaseSeq` 的游标判 `UNAVAILABLE`（410），返回恢复起点；`seq >= hotBaseSeq` 不受影响 |

`trim` 之后对不可用游标的 410 响应体携带：

- `reason=UNAVAILABLE` 与可读 `message`；
- `recoverAfterSeq`：可恢复重放的位置（从该 seq **之后**继续，即热日志第一条事件的前一个 seq）；
- `recoverWindowStart`：恢复起点对应的窗口起点（毫秒）；
- `recoverCursor`：服务端签好的新游标，客户端直接透传即可拿到未压缩后缀的完整事件。

客户端语义：收到 `UNAVAILABLE` 说明那段历史已无法保证与压缩前一致，安全做法是
用 `recoverCursor` 重新订阅，并按需对 `recoverWindowStart` 之后的窗口做一次
区间快照对齐（或整库重建）。游标自身的 TTL 语义（`EXPIRED`）与压缩无关、相互独立。

---

## 2. 一致性保证

- **每标的一把监视器锁**：对同一 `SymbolState` 的所有读写都在 `synchronized(state)` 块内串行化；
  不同标的使用不同监视器，可真正并行。
- 因此同一窗口的 OHLCV 更新不会交错：成交量只在“判重通过后”累加一次，
  不丢失、不重复；并发查询读到的恒为某一笔（或某一串行化批次）之后的完整快照。
- 事件日志在同一把锁内与窗口更新一起追加，seq 连续，查询与拉取不会读到半更新。
- 结算扫描也在同一把锁内、随正序行情推进触发，按窗口起点顺序追加 `SETTLED`；
  压缩（归档 / 清除）在同一把锁内按连续 seq 前缀搬移已结算事件，拉取侧按全局 seq
  合并归档段与热段，因此压缩前后同游标拉取结果不变。
- 并发测试验证：
  - 8 线程 × 4 轮争抢发送同一批 50 笔成交，唯一成交恰好接受 50 次、其余全部 `DUPLICATE`，
    各窗口成交量与手算期望值完全一致；
  - 回补与查询并发时，窗口成交量对查询线程**单调不减**；
  - 游标并发分页拉取，收集到的 seq 集合恰好为 `1..maxSeq`，不重不漏。

---

## 3. 配置项（`src/main/resources/application.properties`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `market-data.window-size-millis` | 1000 | K 线固定窗口大小（毫秒），左闭右开 |
| `market-data.out-of-order-tolerance-millis` | 5000 | 乱序/迟到容忍时长（毫秒），倒退严格超过该值才拒绝 |
| `market-data.cursor-ttl-millis` | 3600000 | 游标有效期（毫秒） |
| `market-data.cursor-secret` | 本地演示默认值 | 游标 HMAC-SHA256 密钥，生产必须覆盖 |
| `market-data.injector-enabled` | true | 启动时是否注入确定性本地行情脚本（测试环境为 false） |

---

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/market-data/symbols/{symbol}/ticks` | 接入一笔逐笔成交 |
| GET | `/api/market-data/symbols/{symbol}/candles?from=&to=` | 查询区间 K 线（含缺口）；from/to 为毫秒时间戳，自动归整到窗口起点 |
| GET | `/api/market-data/symbols/{symbol}/events?cursor=&limit=` | 游标增量拉取（首次不传 cursor） |
| POST | `/api/admin/symbols/{symbol}/compaction?mode=archive\|trim` | 压缩已结算窗口事件：archive=仅归档（旧游标兼容），trim=归档后物理清除（旧游标判 UNAVAILABLE） |

接入请求体：

```json
{ "tradeId": 1001, "timestamp": 1700000000100, "price": 10.0, "volume": 1 }
```

业务级拒绝（重复 / 倒退 / 非法字段）以 200 返回并在 `status` 字段区分；请求级字段缺失返回 400。

K 线与事件内的蜡烛对象统一新增 `settled` 字段；事件类型为
`WINDOW_OPEN / WINDOW_UPDATE / GAP_MARKED / CORRECTION / SETTLED`。

压缩响应体：

```json
{ "symbol": "DEMO", "archivedToSeq": 12, "archivedCount": 10, "hotBaseSeq": 12, "trimmed": true }
```

`trim` 后继续使用旧游标拉取得到 410：

```json
{
  "error": "INVALID_CURSOR",
  "reason": "UNAVAILABLE",
  "message": "游标位置 seq=0 ... 请从可恢复起点 recoverAfterSeq=12（windowStart=...）使用 recoverCursor 继续",
  "recoverAfterSeq": 12,
  "recoverWindowStart": 1700000012000,
  "recoverCursor": "...."
}
```

---

## 5. 本地确定性注入器

`LocalTickInjector` 在应用启动后以固定基准时间 `1700000000000L`（非墙上时钟）推送固定脚本，
每次启动结果可复现，依次覆盖：正常推进 → 重复推送 → 窗口内乱序归位 → 超容忍倒退拒绝 →
字段非法拒绝 → 跨窗口缺口 → 两笔迟到回补缺口 → 再次跨窗口标记新缺口。

启动后典型窗口结果（symbol=`DEMO`，W=1000）：

- w0: open=10 high=11 low=9 close=9 volume=5 corrected v2
- w1 / w2: 先标记缺口，后被回补为正式 K 线（corrected v2）
- w3: open=high=low=close=12 volume=3
- w4: 缺口（gap=true）

注：演示脚本最终水位为 `BASE+5200`，w0 的结算截止线为 `BASE+6000`
（`w+W+tolerance=0+1000+5000`），因此脚本结束时**没有窗口被结算**，
便于直接观察“未结算前仍可修正”的状态；要观察 `SETTLED` 与压缩效果，
参考第 6 节的结算 / 压缩单测（水位推进到截止线之后触发）。

---

## 6. 测试

```bash
mvn test
```

共 37 个用例（均打印输入行情、判定依据与结算 / 压缩判定日志）：

- `IngestIdempotencyTest`：重复推送（含伪造价格的重复）、判重优先于倒退、
  stale/late 可区分、容忍边界闭区间、各类非法字段
- `AggregationWindowTest`：左闭右开边界、窗口内乱序 OHLC 确定性、
  乱序与正序结果一致、跨窗口独立、同时间戳平局规则
- `GapBackfillTest`：缺口标记、迟到回补转正式 K 线、二次回补累加、
  回补判重不双计、事件顺序 WINDOW_OPEN→GAP_MARKED→CORRECTION、超容忍回补被拒
- `CursorPullTest`：首次/重放一致、分页 seq 连续不重不漏、回补后只收 CORRECTION、
  MALFORMED / BAD_SIGNATURE / SYMBOL_MISMATCH / EXPIRED 四类拒绝
- `WindowSettlementTest`（新增）：结算截止线恰好触发、SETTLED 事件顺序与最终快照、
  结算前容忍内迟到仍可 CORRECTION / 结算后迟到判 STALE 且窗口冻结、
  缺口以 gap=true 结算且不可污染、同一脚本两遍结算 seq/窗口顺序完全一致
- `EventReplayConsistencyTest`（新增）：全量事件重放（含 WINDOW_UPDATE、缺口、回补、
  结算）逐窗口逐字段等于区间快照；在每个有效游标位置重放，游标后交付的窗口与快照一致；
  同一游标用不同分页大小重放结果稳定；**一个写线程持续跨窗口写入并回补、
  一个客户端线程持续增量折叠重放，过程中无倒退且写入结束后全量重放与最终快照一致**
- `CompactionCursorTest`（新增）：仅归档后从最早游标 / 归档内游标继续拉取，
  事件序列与压缩前完全一致（不漏不重），重复归档幂等；物理清除后旧游标判
  UNAVAILABLE 且携带 recoverAfterSeq / recoverWindowStart / recoverCursor，
  用恢复游标继续拿到与未压缩一致的完整后缀；压缩后继续写入新 seq 接续不复用
- `ConcurrentReadWriteTest`：并发重复写不丢不重、并发读单调不减、并发游标拉取不重不漏
  （拉取线程在首次追平时等待回补线程结束后复核，消除“提前退出漏事件”的时序误报）

### 6.1 本地手工验证结算与压缩

启动服务（注入器默认开启）后，也可用如下确定性步骤手工验证（W=1000、tolerance=5000）：

```bash
# 1) 开窗 w0 并把水位推到其结算截止线 6000（w0 即刻结算）
curl -s -XPOST localhost:8080/api/market-data/symbols/M/ticks \
  -H 'Content-Type: application/json' \
  -d '{"tradeId":1,"timestamp":1700000000000,"price":10,"volume":1}'
curl -s -XPOST localhost:8080/api/market-data/symbols/M/ticks \
  -H 'Content-Type: application/json' \
  -d '{"tradeId":2,"timestamp":1700000006000,"price":11,"volume":1}'
# 2) 事件流里应能看到 w0 的 SETTLED，K 线里 w0 的 settled=true
curl -s 'localhost:8080/api/market-data/symbols/M/events?limit=100'
# 3) 仅归档：记录返回的旧游标后压缩，旧游标仍可完整回放
curl -s -XPOST 'localhost:8080/api/admin/symbols/M/compaction?mode=archive'
# 4) 物理清除：早于 hotBaseSeq 的游标返回 410 UNAVAILABLE 与 recoverCursor
curl -s -XPOST 'localhost:8080/api/admin/symbols/M/compaction?mode=trim'
```

也可以直接按类名单独运行：

```bash
mvn test -Dtest=WindowSettlementTest
mvn test -Dtest=EventReplayConsistencyTest
mvn test -Dtest=CompactionCursorTest
```
