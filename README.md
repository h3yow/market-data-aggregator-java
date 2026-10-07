# 本地行情接入与 K 线聚合服务

在 Spring Boot 3 / Java 21 骨架上实现的**本地、确定性**行情接入与固定窗口 K 线聚合服务。
用本地注入的逐笔成交模拟数据源，使**乱序、重复、缺口**场景下的聚合结果确定、可解释、可复现。

- 多标的逐笔行情接入，重复 / 时间戳倒退 / 乱序按明确规则接受或拒绝，结果可区分
- 固定时间窗口聚合 OHLCV（开高低收、成交量），窗口归属与边界唯一确定
- 无行情窗口缺口标记；迟到数据可回补修正且不重复累计
- 基于签名游标的增量拉取：重复拉取不重不漏，过期 / 非法 / 串标游标明确拒绝
- 事件流可独立重建视图：从任意有效游标重放，每个窗口的 OHLCV / 缺口 / 结算状态与区间快照查询完全一致
- 窗口结算：越过“窗口终点 + 乱序容忍”即明确结算（`WINDOW_SETTLED`），之后不再被修正，客户端可安全落库
- 事件日志可压缩：已结算窗口折叠为单条结算事件且 seq 不重编号；压缩后游标按“压缩前沿”
  二分兼容——位置存活的游标逐条一致，位置被压缩的游标被明确拒绝（`COMPACTED`/410）并附带
  可恢复起点与新游标，按恢复重放得到的窗口状态与区间查询完全一致（见 1.7）
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

### 1.5 增量游标（不重不漏）

- 每个标的维护**只追加**的事件日志，`seq` 从 1 单调递增，事件类型：
  `WINDOW_OPEN`（窗口首次产生数据）、`WINDOW_UPDATE`（开放窗口并入正序行情，快照刷新）、
  `GAP_MARKED`（缺口标记）、`CORRECTION`（迟到数据修正窗口，含缺口回补）、
  `WINDOW_SETTLED`（窗口结算，携带最终快照，见 1.6）。
- **事件流可独立重建视图**：每一次窗口状态变化都会追加一条携带完整快照的事件，
  客户端从任意有效游标开始按 seq 顺序重放（同一窗口后到的快照覆盖先到的），
  得到的每个窗口 OHLCV / 缺口 / corrected / version / settled 与直接按区间查 K 线完全一致；
  同一游标重放任意多次结果稳定。
- 游标为不透明签名串：`base64url(symbol|seq|issuedAt) + "." + base64url(HMAC-SHA256(payload))`。
  游标中的 `seq` 表示“已交付到该序号”，服务端始终返回 `event.seq > cursor.seq` 的事件，
  因此同一游标重复拉取结果完全一致（不重不漏）。
- 窗口一旦开窗 / 更新 / 标缺口 / 被修正 / 结算，即不可变地追加事件；事件与窗口更新在同一把锁内完成，不会读到半更新。
- 非法游标按原因区分拒绝：

  | reason | 条件 | HTTP |
  | --- | --- | --- |
  | `MALFORMED` | 非两段式、base64 不可解码、载荷字段数/数字非法、seq 为负 | 400 |
  | `BAD_SIGNATURE` | HMAC 校验失败（被篡改 / 密钥不符） | 400 |
  | `SYMBOL_MISMATCH` | 游标属于其他标的 | 400 |
  | `EXPIRED` | 超过 `cursor-ttl-millis` | 410 Gone |
  | `COMPACTED` | 续拉位置的事件已被压缩删除（`seq < compactedThroughSeq`），无法逐条一致；响应带 `resumeSeq`/`resumeCursor` 恢复起点 | 410 Gone |

### 1.6 窗口结算（唯一确定、可复现）

- 窗口 `X` 已结算 ⟺ 当前水位 `watermark >= X.windowStart + W + out-of-order-tolerance-millis`。
- 含义：此时窗口内任何可能时间戳 `ts < X.windowStart + W` 都有 `watermark - ts > tolerance`，
  即该窗口的迟到数据**必然**被判 `REJECTED_STALE`，窗口不再可能被修正；
  同时水位已越过窗口终点，必然已被更晚的窗口推进。结算规则与迟到 / 回补容忍规则严格一致。
- 判定只是“已接受行情序列”的纯函数，与墙上时钟无关，重放同一输入得到同一结算序列。
- 每次水位推进后按窗口起点升序结算，每个窗口至多结算一次，追加 `WINDOW_SETTLED` 事件
  （携带该窗口最终快照）；快照查询中同一窗口的 `settled` 字段同步置真。
- 客户端可凭事件类型区分窗口生命周期：
  `WINDOW_OPEN`（刚出现）→ `WINDOW_UPDATE`（开放中刷新）→ `GAP_MARKED`（变成缺口）→
  `CORRECTION`（被迟到数据修正）→ `WINDOW_SETTLED`（已结算，可安全落库）。
  只有收到 `WINDOW_SETTLED`（或快照中 `settled=true`）的窗口才保证不再变更。

### 1.7 事件压缩、归档与游标兼容范围

- 压缩是显式运维动作：`POST /api/market-data/symbols/{symbol}/compact`
  （或调用 `MarketDataService.compact`），不在接入路径上自动执行。
- 规则：**只折叠已结算窗口**的历史事件——每个已结算窗口仅保留其 `WINDOW_SETTLED` 事件
  （恒为该窗口最后一条事件，携带最终快照），其余事件删除；未结算窗口的事件一律不动。
  **所有保留事件的 seq 不重编号**，新事件继续接原序号递增，绝不复用被删号段。
- 每次压缩记录该标的的**压缩前沿** `compactedThroughSeq`（历次删除事件 seq 的高水位，
  单调不减），并在压缩报告中返回。游标兼容范围由它严格二分：

  | 情形 | 服务行为 |
  | --- | --- |
  | 请求**不带游标**（首次拉取 / 恢复） | 始终从**当前压缩后日志开头**拉取，永不报错；这就是压缩恢复路径 |
  | 常规游标 `seq >= compactedThroughSeq` | 游标之后的事件一条未删，续拉序列与“从未压缩”时**逐条一致**（不重不漏） |
  | 常规游标 `seq < compactedThroughSeq` | 续拉位置内的事件已被删除，无法保证逐条一致：以 `COMPACTED` 明确拒绝（HTTP 410），响应带 `resumeSeq`、`resumeCursor` 与原因说明，绝不静默返回对不上的半截序列 |
  | 恢复游标（4 段签名游标） | 只承诺最终视图一致：从压缩后日志重放，重放期间继续压缩也不会被拒（必要时服务端透明快进到当前日志开头） |

- **收到 `COMPACTED` 后的恢复协议**：丢弃本地基于增量事件维护的状态，
  用响应中的 `resumeCursor`（或干脆不带 cursor）从头重放。每个已结算窗口都保留了一条
  `WINDOW_SETTLED` 最终快照、未结算窗口事件未动，因此重放重建的**每个窗口最终状态
  与区间快照查询完全一致**（缺口也不丢：缺口结算时同样保留其结算事件）。
- 为什么重放不会重复/错乱：压缩与接入、拉取在同一把 `synchronized(state)` 临界区内，
  seq 只追加不补号；恢复游标看到的保留事件集合虽然只是原流的“结算快照子集”，
  但对同一窗口“后到快照覆盖先到”的折叠规则而言，最终视图与完整流逐条相等。
- 压缩幂等：对已压缩日志再次执行不删除任何事件，`compactedThroughSeq` 不回退；
  反复压缩 + 持续写入 + 多客户端持续拉取下，每个客户端对同一 seq 至多收到一次、
  seq 只向前，最终重建视图与 K 线区间查询一致（见并发用例）。
- 其它兼容边界：游标仍受 `cursor-ttl-millis` 限制，超 TTL 照旧以 `EXPIRED`(410) 拒绝
  （与 `COMPACTED` 互斥、可区分）；`MALFORMED`/`BAD_SIGNATURE`/`SYMBOL_MISMATCH`
  仍为 400。

#### 压缩报告字段

`POST .../compact` 返回：

| 字段 | 含义 |
| --- | --- |
| `settledWindows` | 参与压缩的已结算窗口数 |
| `droppedEvents` | 本次删除的事件数（重复压缩为 0） |
| `keptEvents` | 压缩后保留的事件数 |
| `maxDroppedSeq` | 本次被删事件最大 seq（无删除为 0） |
| `compactedThroughSeq` | **压缩前沿**：游标逐条一致 / 明确不可用的分界点（单调不减） |

#### `COMPACTED` 错误响应（HTTP 410）

```json
{
  "error": "INVALID_CURSOR",
  "reason": "COMPACTED",
  "message": "游标续拉位置 seq=3 所在的事件已被压缩删除（压缩前沿 seq=18），无法再保证与压缩前逐条一致；请丢弃本地增量状态，改用返回的新游标（resumeSeq=18，即压缩后日志开头）重放以恢复全部窗口最终状态",
  "resumeSeq": 18,
  "resumeCursor": "payload.signature（4 段恢复游标，不透明串，直接透传）"
}
```

---

## 2. 一致性保证

- **每标的一把监视器锁**：对同一 `SymbolState` 的所有读写都在 `synchronized(state)` 块内串行化；
  不同标的使用不同监视器，可真正并行。
- 因此同一窗口的 OHLCV 更新不会交错：成交量只在“判重通过后”累加一次，
  不丢失、不重复；并发查询读到的恒为某一笔（或某一串行化批次）之后的完整快照。
- 事件日志在同一把锁内与窗口更新一起追加，seq 连续，查询与拉取不会读到半更新。
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
| POST | `/api/market-data/symbols/{symbol}/compact` | 压缩事件日志：折叠已结算窗口的历史事件（seq 不重编号），返回压缩统计（含压缩前沿 `compactedThroughSeq`） |

接入请求体：

```json
{ "tradeId": 1001, "timestamp": 1700000000100, "price": 10.0, "volume": 1 }
```

业务级拒绝（重复 / 倒退 / 非法字段）以 200 返回并在 `status` 字段区分；请求级字段缺失返回 400。

---

## 5. 本地确定性注入器

`LocalTickInjector` 在应用启动后以固定基准时间 `1700000000000L`（非墙上时钟）推送固定脚本，
每次启动结果可复现，依次覆盖：正常推进 → 重复推送 → 窗口内乱序归位 → 超容忍倒退拒绝 →
字段非法拒绝 → 跨窗口缺口 → 两笔迟到回补缺口 → 再次跨窗口标记新缺口 →
大幅推进水位触发早期窗口结算（`WINDOW_SETTLED`）。

启动后典型窗口结果（symbol=`DEMO`，W=1000，tolerance=5000）：

- w0: open=10 high=11 low=9 close=9 volume=5 corrected v2，已结算（settled=true）
- w1 / w2: 先标记缺口，后被回补为正式 K 线（corrected v2），已结算
- w3: open=high=low=close=12 volume=3，已结算
- w4: 缺口（gap=true），已结算——缺口也可以结算，结算后缺口即最终状态
- w5: open=high=low=close=13 volume=4，已结算
- w6..w11: 缺口（w6 已结算，w7..w11 未结算）
- w12: open=high=low=close=14 volume=2，未结算（当前开放窗口）

---

## 6. 测试

```bash
mvn test
```

共 40 个用例（均打印输入行情与判定依据日志）：

- `IngestIdempotencyTest`：重复推送（含伪造价格的重复）、判重优先于倒退、
  stale/late 可区分、容忍边界闭区间、各类非法字段
- `AggregationWindowTest`：左闭右开边界、窗口内乱序 OHLC 确定性、
  乱序与正序结果一致、跨窗口独立、同时间戳平局规则
- `GapBackfillTest`：缺口标记、迟到回补转正式 K 线、二次回补累加、
  回补判重不双计、事件顺序 WINDOW_OPEN→GAP_MARKED→CORRECTION、超容忍回补被拒
- `CursorPullTest`：首次/重放一致、分页 seq 连续不重不漏、回补后只收 CORRECTION、
  MALFORMED / BAD_SIGNATURE / SYMBOL_MISMATCH / EXPIRED 四类拒绝
- `EventReplayConsistencyTest`：从头 / 中间游标重放事件流与区间快照查询逐窗口一致、
  同一游标重放稳定、边写边拉追平后收敛到快照且 seq 不重不漏
- `WindowSettlementTest`：结算边界恰好为“窗口终点+容忍”（差 1ms 不结算）、
  结算前可修正 / 结算后必拒、缺口窗口同样结算、生命周期事件可区分、
  同一脚本两遍事件流逐条一致（可复现）
- `EventCompactionTest`（4 用例）：
  ①同游标位置存活时压缩前后拉取的事件序列**逐条一致**（seq=18/21 两个代表位置，
  与独立的“未压缩世界”逐事件比对，不漏不重）；
  ②游标续拉位置被压缩时以 **COMPACTED 明确拒绝**（区分于正常拉取），错误带原因、
  `resumeSeq`=压缩后日志开头、`resumeCursor`，用新游标重放收敛到区间查询且已结算窗口一个不少，
  并覆盖 seq=0/1/17 等删除区间内位置；
  ③反复压缩幂等（第二、三次 dropped=0）、前沿单调不减、压缩后新事件 seq 不复用被删号段；
  ④持续写入叠加反复压缩与 4 个客户端持续拉取（含真实 COMPACTED 重置/恢复路径），
  每个客户端同一条事件不重复、seq 只向前，最终重建视图与区间查询完全一致
- `CompactionCursorHttpErrorTest`：COMPACTED 在 HTTP 层为 410 Gone，响应体含
  reason/message/resumeSeq/resumeCursor；MALFORMED 仍为 400 且不带恢复字段
- `ConcurrentReadWriteTest`：并发重复写不丢不重、并发读单调不减、并发游标拉取不重不漏

---

## 7. 本地验证

```bash
# 1. 启动（自动注入确定性脚本，symbol=DEMO）
mvn spring-boot:run

# 2. 从头拉取事件流，观察窗口生命周期事件（OPEN/UPDATE/GAP/CORRECTION/SETTLED）
curl -s 'http://localhost:8080/api/market-data/symbols/DEMO/events?limit=1000'

# 3. 与区间快照查询对比：同一窗口的最终状态必然一致
curl -s 'http://localhost:8080/api/market-data/symbols/DEMO/candles'

# 4. 压缩事件日志，返回统计；重点看 compactedThroughSeq（压缩前沿）
curl -s -X POST 'http://localhost:8080/api/market-data/symbols/DEMO/compact'

# 5a. 【位置存活的旧游标】拿第 2 步签发、且 seq >= compactedThroughSeq 的 nextCursor 续拉：
#     不报错，事件序列与压缩前逐条一致，不重不漏
# 5b. 【位置被压缩的旧游标】拿一个“落在删除区间内”的旧游标续拉，会被明确拒绝：
#     HTTP 410，reason=COMPACTED，响应同时给出 resumeSeq 与 resumeCursor，例如：
#     {"reason":"COMPACTED","resumeSeq":...,"resumeCursor":"....", ...}
curl -i 'http://localhost:8080/api/market-data/symbols/DEMO/events?cursor=<落在删除区间的旧游标>'

# 6. 恢复：丢弃本地增量状态，用返回的 resumeCursor（或干脆不带 cursor）重放，
#    重建出的每个窗口最终状态与第 3 步区间快照完全一致；恢复期间继续压缩也不会再被拒
curl -s 'http://localhost:8080/api/market-data/symbols/DEMO/events?cursor=<resumeCursor>&limit=1000'
```
