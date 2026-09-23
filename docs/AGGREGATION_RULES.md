# 本地行情接入与 K 线聚合服务 — 规则与一致性说明

## 1. 总览

- 数据形态：本地注入的逐笔成交行情 `Tick{symbol, tradeId, price, volume, tradeTime}`。
- 存储形态：进程内、按标的（symbol）隔离的窗口表与变更日志（changelog）。
- 提供能力：
  1. 多标的逐笔行情接入，明确区分接受 / 拒绝原因；
  2. 固定时间窗口 K 线聚合（OHLCV）；
  3. 无行情窗口的缺口（GAP）标记与迟到回补（BACKFILLED）；
  4. 基于签名游标的增量拉取，重复拉取不重不漏。

## 2. 窗口归属与边界规则（唯一确定）

- 窗口长度为 `market-data.window-ms`（默认 1000ms）。
- 一笔成交时间 `t` 归属窗口起点：
  `windowStart = floor(t / windowMs) * windowMs`，窗口为 **左闭右开**
  `[windowStart, windowStart + windowMs)`。
  - 边界点 `t == windowStart + windowMs` 归属右侧窗口（例如 1000ms 属于
    `[1000,2000)`，不属于 `[0,1000)`）。
  - 对历史负时间戳使用 `floorDiv` 语义：`-1ms` 属于 `[-1000, 0)`。
- 窗口 OHLCV 由该窗口内**全部已接受行情**按 `(tradeTime 升序, tradeId 升序)`
  确定性排序后重算：
  - open = 首笔成交价，close = 末笔成交价；
  - high / low = 全部成交价的最大 / 最小值；
  - volume = 各笔成交量之和。
- 因为是"全量重算"而非增量累加，乱序到达、迟到回补只会修正结果，
  **不会重复累计成交量**；到达顺序不影响最终结果。

## 3. 行情接入判定规则

接入处理严格按以下顺序执行，每种结果都可通过 `decision` 字段区分：

| decision | 含义 | 是否计入 |
| --- | --- | --- |
| `ACCEPTED_NEW` | 接受：首次创建（推进前沿的）新窗口 | 是 |
| `ACCEPTED_LATE_FILL` | 接受：对已有窗口的写入——前沿窗口内乱序到达、旧窗口迟到修正或缺口回补 | 是 |
| `REJECTED_DUPLICATE` | 同一 `(symbol, tradeId)` 已处理过（即使重发内容被篡改也拒绝） | 否 |
| `REJECTED_STALE` | 时间戳倒退/过旧：`referenceNow - tradeTime > market-data.max-late-ms` | 否 |
| `REJECTED_INVALID` | 字段非法：symbol/tradeId 空白、price≤0、volume≤0、tradeTime<0 | 否 |

规则要点：

1. **幂等键是 `(symbol, tradeId)`**。判重与写入在同一把标的锁内原子完成，
   不存在"检查通过但尚未写入"的并发窗口；重复推送永远不会二次计入。
2. **时间戳倒退**分两种处理：
   - 在容忍窗口 `max-late-ms` 内：接受为迟到数据，进入所属窗口重算聚合，
     结果标记 `ACCEPTED_LATE_FILL`；
   - 超出容忍窗口：明确拒绝为 `REJECTED_STALE`，返回中给出滞后毫秒数与阈值。
3. 拒绝结果（重复/过旧/非法）均**不写状态、不产生变更日志**。

## 4. 缺口标记与回补规则

- 为每个标的维护"已观测到的最高窗口起点 frontier"。
- 当一笔行情把 frontier 向前推进到 `w` 时，frontier 与 `w` 之间所有没有数据的
  窗口被**物化为缺口**：`status=GAP`，OHLCV 为空、volume=0，并各产生一条变更事件。
- **首个已观测窗口之前的历史时间不视为缺口**（服务没有任何更早的行情可参照，
  凭空假设缺口不可解释）。
- 迟到数据落入一个 `GAP` 窗口后：
  - 窗口状态变为 `BACKFILLED`，OHLCV 按该窗口行情重算；
  - 产生一条新的变更事件，客户端通过增量拉取即可观察到
    `GAP -> BACKFILLED` 的修正；
  - 同窗口后续再有迟到数据，状态保持 `BACKFILLED`，继续确定性重算；
  - 因幂等去重，回补数据不会被重复累计。
- 窗口状态取值：`FILLED`（正常有数据）、`GAP`（无行情缺口）、
  `BACKFILLED`（缺口已被迟到数据回补）。

## 5. 增量拉取与游标

- 每次窗口被新建、更新、标记缺口或回补，都会向该标的的 changelog 追加一条
  带单调递增 `seq` 的 `CandleChange` 事件（事件中带窗口快照）。
- 拉取接口返回 `seq` **严格大于**游标中 seq 的事件，按 seq 升序分页：
  - 不传游标：从 seq=0 开始（全量增量）；
  - 重复使用同一游标：返回空集，游标保持不变 —— **不重不漏**；
  - 回补产生新事件后，用旧游标续拉只能得到新增的修正事件。
- 游标内容为 `symbol|lastSeq|issuedAt` 的 base64url 载荷 + HMAC-SHA256 签名：
  - 签名不匹配 / 格式损坏 → `400 INVALID_CURSOR`；
  - 游标中的标的与路径标的不一致 → 明确拒绝（跨标的复用）；
  - 签发时间超过 `market-data.cursor-ttl-ms` → 明确拒绝（过期）；
  - seq 为负、签发时间在未来等 → 明确拒绝。

## 6. 一致性保证

- 每个标的一把独立锁，锁内完成：幂等判重、窗口写入/缺口物化、OHLCV 重算、
  changelog 追加 —— 对同一标的的接入完全串行化。
- 判重集合与窗口数据在同一临界区内更新，杜绝重复计入；成交量由事件集合求和，
  无增量累加误差。
- 查询（快照读取与增量拉取）同样在标的锁内进行，并返回深拷贝快照，
  读操作不会读到半成品，也不会与后续写入互相干扰。
- 不同标的之间互不阻塞。
- 确定性：给定同一批行情（无论到达顺序），最终窗口 OHLCV 与缺口结论一致。

## 7. HTTP 接口

### 接入行情
`POST /api/market-data/ticks`
```json
{ "symbol": "600000", "tradeId": "T1", "price": 100, "volume": 10, "tradeTime": 1700000000100 }
```
响应含 `accepted / decision / reason / windowStart / changeSeq`。
拒绝原因可区分：`REJECTED_DUPLICATE` / `REJECTED_STALE` / `REJECTED_INVALID`。

### 增量拉取
`GET /api/market-data/symbols/{symbol}/candles/pull?cursor=<可选>`

响应：
```json
{
  "symbol": "600000",
  "changes": [
    { "seq": 1, "windowStart": 0, "status": "FILLED",
      "candle": { "openPrice": 100, "highPrice": 110, "lowPrice": 90,
                  "closePrice": 105, "volume": 30, "...": "..." } },
    { "seq": 2, "windowStart": 1000, "status": "GAP", "candle": { "volume": 0 } }
  ],
  "nextCursor": "...",
  "hasMore": false
}
```
非法/过期/跨标的游标返回 `400`：
```json
{ "error": "INVALID_CURSOR", "message": "cursor has expired" }
```

## 8. 配置项（application.properties）

| 配置键 | 默认值 | 说明 |
| --- | --- | --- |
| `market-data.window-ms` | `1000` | K 线窗口大小（毫秒），必须为正 |
| `market-data.max-late-ms` | `5000` | 允许的行情迟到/时间戳倒退上限（毫秒），超过拒绝为 STALE |
| `market-data.cursor-ttl-ms` | `86400000` | 游标有效期（毫秒），过期游标被拒绝 |
| `market-data.cursor-secret` | `change-me-in-production` | 游标 HMAC 签名密钥，生产环境必须修改 |
| `market-data.pull-batch-size` | `500` | 单次增量拉取最多返回的变更条数 |

非法配置（如窗口长度非正）会在启动时快速失败。

## 9. 日志与测试

- 接入层对每笔行情打印 `[TICK-ACCEPT]` / `[TICK-REJECT]`，包含完整输入字段、
  判定 decision、所属窗口与判定依据（含滞后毫秒数、阈值、重复 tradeId 等）。
- 测试覆盖：重复推送（含内容篡改）、乱序与迟到、跨窗口边界（含负时间）、
  缺口物化与回补不重复累计、游标分页/重拉/非法/过期/跨标的、
  以及多线程并发接入与并发拉取。测试用例自身也打印输入行情与断言结论。
