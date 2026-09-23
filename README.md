# 本地行情接入与 K 线聚合服务

在 Spring Boot 3 / Java 21 骨架上实现的**本地、确定性**行情接入与固定窗口 K 线聚合服务。
用本地注入的逐笔成交模拟数据源，使**乱序、重复、缺口**场景下的聚合结果确定、可解释、可复现。

- 多标的逐笔行情接入，重复 / 时间戳倒退 / 乱序按明确规则接受或拒绝，结果可区分
- 固定时间窗口聚合 OHLCV（开高低收、成交量），窗口归属与边界唯一确定
- 无行情窗口缺口标记；迟到数据可回补修正且不重复累计
- 基于签名游标的增量拉取：重复拉取不重不漏，过期 / 非法 / 串标游标明确拒绝
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
  `WINDOW_OPEN`（窗口首次产生数据）、`GAP_MARKED`（缺口标记）、
  `CORRECTION`（迟到数据修正窗口，含缺口回补）。
- 游标为不透明签名串：`base64url(symbol|seq|issuedAt) + "." + base64url(HMAC-SHA256(payload))`。
  游标中的 `seq` 表示“已交付到该序号”，服务端始终返回 `event.seq > cursor.seq` 的事件，
  因此同一游标重复拉取结果完全一致（不重不漏）。
- 开放窗口内的实时更新不产生事件（仍可通过 K 线快照查询观察）；窗口一旦开窗/标缺口/被修正即不可变地追加事件。
- 非法游标按原因区分拒绝：

  | reason | 条件 | HTTP |
  | --- | --- | --- |
  | `MALFORMED` | 非两段式、base64 不可解码、载荷字段数/数字非法、seq 为负 | 400 |
  | `BAD_SIGNATURE` | HMAC 校验失败（被篡改 / 密钥不符） | 400 |
  | `SYMBOL_MISMATCH` | 游标属于其他标的 | 400 |
  | `EXPIRED` | 超过 `cursor-ttl-millis` | 410 Gone |

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

接入请求体：

```json
{ "tradeId": 1001, "timestamp": 1700000000100, "price": 10.0, "volume": 1 }
```

业务级拒绝（重复 / 倒退 / 非法字段）以 200 返回并在 `status` 字段区分；请求级字段缺失返回 400。

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

---

## 6. 测试

```bash
mvn test
```

共 26 个用例（均打印输入行情与判定依据日志）：

- `IngestIdempotencyTest`：重复推送（含伪造价格的重复）、判重优先于倒退、
  stale/late 可区分、容忍边界闭区间、各类非法字段
- `AggregationWindowTest`：左闭右开边界、窗口内乱序 OHLC 确定性、
  乱序与正序结果一致、跨窗口独立、同时间戳平局规则
- `GapBackfillTest`：缺口标记、迟到回补转正式 K 线、二次回补累加、
  回补判重不双计、事件顺序 WINDOW_OPEN→GAP_MARKED→CORRECTION、超容忍回补被拒
- `CursorPullTest`：首次/重放一致、分页 seq 连续不重不漏、回补后只收 CORRECTION、
  MALFORMED / BAD_SIGNATURE / SYMBOL_MISMATCH / EXPIRED 四类拒绝
- `ConcurrentReadWriteTest`：并发重复写不丢不重、并发读单调不减、并发游标拉取不重不漏
