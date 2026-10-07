package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.EventType;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestStatus;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 多标的内存聚合存储。
 *
 * <p>一致性模型：每个标的一个 {@link SymbolState}，对该标的的读与写全部在
 * {@code synchronized(state)} 块内串行化；不同标的互不阻塞。
 * 因此同一窗口的 OHLCV 永远不会交错更新，成交量不丢失、不重复，
 * 并发查询看到的恒为某一笔（或某一批串行化后）的完整快照。
 *
 * <p>接入判定优先级（互斥、可解释）：
 * <ol>
 *   <li>{@link IngestStatus#DUPLICATE}：(symbol, tradeId) 已接受过——无论时间戳多旧，
 *       重放必须幂等且可识别，故最先判定；</li>
 *   <li>{@link IngestStatus#REJECTED_STALE}：时间戳早于水位且超出乱序容忍；</li>
 *   <li>{@link IngestStatus#ACCEPTED_LATE}：时间戳早于水位但在容忍范围内，按时间戳归位，
 *       落入已存在窗口即产生版本化修正（CORRECTION）；</li>
 *   <li>{@link IngestStatus#ACCEPTED}：时间戳不早于水位，正常推进；跨窗口时先补缺口标记。</li>
 * </ol>
 * 字段合法性校验在 {@link IngestService} 完成，不进入本类。
 */
@Component
public class AggregationStore {

    private static final Logger log = LoggerFactory.getLogger(AggregationStore.class);

    private final MarketDataProperties properties;
    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();

    public AggregationStore(MarketDataProperties properties) {
        this.properties = properties;
    }

    private long size() {
        return properties.getWindowSizeMillis();
    }

    private long tolerance() {
        return properties.getOutOfOrderToleranceMillis();
    }

    public SymbolState stateFor(String symbol) {
        return states.computeIfAbsent(symbol, SymbolState::new);
    }

    /**
     * 接受一笔已通过字段校验的行情，返回可区分的判定结果。
     */
    public IngestResult accept(TradeTick tick) {
        long windowSize = size();
        long ws = WindowClock.windowStart(tick.timestamp(), windowSize);
        SymbolState state = stateFor(tick.symbol());
        synchronized (state) {
            // 1) 幂等：重复推送（同一 symbol+tradeId），即使时间戳很旧也先判重
            if (state.seenTradeIds.contains(tick.tradeId())) {
                return result(IngestStatus.DUPLICATE,
                        "tradeId=" + tick.tradeId() + " 已处理，重复推送被忽略，不重复计入成交量",
                        null, tick);
            }

            // 2) 时间戳倒退判定（仅在已有水位之后有意义）
            if (state.maxWindowStart != null && tick.timestamp() < state.watermark) {
                long lag = state.watermark - tick.timestamp();
                if (lag > tolerance()) {
                    return result(IngestStatus.REJECTED_STALE,
                            "时间戳倒退 " + lag + "ms 超过乱序容忍 " + tolerance()
                                    + "ms（水位=" + state.watermark + "），拒绝且不修改任何窗口",
                            null, tick);
                }
                return applyLate(state, tick, ws, lag, windowSize);
            }

            // 3) 正常接受（首笔或时间戳不早于水位）
            return applyInOrder(state, tick, ws, windowSize);
        }
    }

    /**
     * 乱序/迟到但在容忍范围内：按时间戳归入对应窗口并修正。
     */
    private IngestResult applyLate(SymbolState state, TradeTick tick, long ws, long lag, long windowSize) {
        SymbolState.WindowAgg agg = state.windows.get(ws);
        if (agg == null) {
            // 早于已知最早窗口：此前没有任何该窗口的结论，直接开窗，不推断更早的缺口
            agg = new SymbolState.WindowAgg(ws);
            state.windows.put(ws, agg);
            agg.apply(tick.timestamp(), tick.price(), tick.volume());
            state.seenTradeIds.add(tick.tradeId());
            appendEvent(state, EventType.WINDOW_OPEN, agg, windowSize);
            return result(IngestStatus.ACCEPTED_LATE,
                    "迟到 " + lag + "ms 落入此前未观测的更早窗口 " + ws + "，新建窗口，水位不变",
                    ws, tick);
        }
        boolean wasGap = agg.gap;
        agg.apply(tick.timestamp(), tick.price(), tick.volume());
        state.seenTradeIds.add(tick.tradeId());
        agg.corrected = true;
        agg.version += 1;
        appendEvent(state, EventType.CORRECTION, agg, windowSize);
        String why = wasGap
                ? "迟到 " + lag + "ms 回补缺口窗口 " + ws + "：缺口标记转为正式K线，版本升至 " + agg.version
                : "迟到 " + lag + "ms 修正已聚合窗口 " + ws + "：OHLCV 按时间戳重算，版本升至 " + agg.version;
        return result(IngestStatus.ACCEPTED_LATE, why + "，水位不变", ws, tick);
    }

    /**
     * 正常接受：推进水位，跨窗口时先为中间无行情窗口打缺口标记，再聚合当前窗口。
     */
    private IngestResult applyInOrder(SymbolState state, TradeTick tick, long ws, long windowSize) {
        if (state.maxWindowStart == null) {
            state.maxWindowStart = ws;
            state.watermark = tick.timestamp();
            SymbolState.WindowAgg agg = new SymbolState.WindowAgg(ws);
            state.windows.put(ws, agg);
            agg.apply(tick.timestamp(), tick.price(), tick.volume());
            state.seenTradeIds.add(tick.tradeId());
            appendEvent(state, EventType.WINDOW_OPEN, agg, windowSize);
            return result(IngestStatus.ACCEPTED, "首笔行情，窗口 " + ws + " 开窗", ws, tick);
        }

        long prevMax = state.maxWindowStart;
        state.watermark = tick.timestamp();

        if (ws == prevMax) {
            // 仍在当前开放窗口内：更新快照并下发 WINDOW_UPDATE，
            // 使事件流重放能得到与快照查询完全一致的窗口最新状态
            SymbolState.WindowAgg agg = state.windows.get(ws);
            agg.apply(tick.timestamp(), tick.price(), tick.volume());
            state.seenTradeIds.add(tick.tradeId());
            appendEvent(state, EventType.WINDOW_UPDATE, agg, windowSize);
            settleWindows(state, windowSize);
            return result(IngestStatus.ACCEPTED, "时间戳不早于水位，并入当前开放窗口 " + ws, ws, tick);
        }

        // 跨越了一个或多个窗口：严格位于 (prevMax, ws) 之间、无任何行情的窗口标记为缺口
        for (long gapStart = prevMax + windowSize; gapStart < ws; gapStart += windowSize) {
            SymbolState.WindowAgg gap = new SymbolState.WindowAgg(gapStart);
            state.windows.put(gapStart, gap); // gap.gap 保持 true
            appendEvent(state, EventType.GAP_MARKED, gap, windowSize);
        }

        state.maxWindowStart = ws;
        SymbolState.WindowAgg agg = new SymbolState.WindowAgg(ws);
        state.windows.put(ws, agg);
        agg.apply(tick.timestamp(), tick.price(), tick.volume());
        state.seenTradeIds.add(tick.tradeId());
        appendEvent(state, EventType.WINDOW_OPEN, agg, windowSize);
        settleWindows(state, windowSize);
        long gapCount = (ws - prevMax) / windowSize - 1;
        return result(IngestStatus.ACCEPTED,
                "水位推进至窗口 " + ws + "，中间标记缺口窗口 " + gapCount + " 个，新窗口开窗",
                ws, tick);
    }

    /**
     * 窗口结算判定（唯一确定、可复现）：
     * 窗口 X 已结算 ⟺ 当前水位 {@code watermark >= X.windowStart + W + tolerance}。
     *
     * <p>此时该窗口内任何可能的时间戳 ts（&lt; X.windowStart + W）都有
     * {@code watermark - ts > tolerance}，即任何迟到数据都会被判 {@link IngestStatus#REJECTED_STALE}，
     * 窗口不再可能被修正；同时水位已越过窗口终点，必然存在更晚的窗口推进了它。
     * 与乱序容忍规则严格一致，且只是已接受行情序列的纯函数，重放结果可复现。
     *
     * <p>每次水位推进后按窗口起点升序扫描，每个窗口至多结算一次，
     * 结算时追加 {@link EventType#WINDOW_SETTLED} 事件（携带该窗口最终快照）。
     */
    private void settleWindows(SymbolState state, long windowSize) {
        long settleBefore = state.watermark - tolerance() - windowSize;
        for (SymbolState.WindowAgg agg : state.windows.values()) {
            if (agg.settled) {
                continue;
            }
            if (agg.windowStart > settleBefore) {
                break; // 窗口按起点升序，后续窗口只会更晚，都不可能结算
            }
            agg.settled = true;
            appendEvent(state, EventType.WINDOW_SETTLED, agg, windowSize);
            log.info("[SETTLE] symbol={} window={} 已结算：水位={} >= 窗口终点+容忍={}，窗口不再接受修正",
                    state.symbol, agg.windowStart, state.watermark,
                    agg.windowStart + windowSize + tolerance());
        }
    }

    private void appendEvent(SymbolState state, EventType type, SymbolState.WindowAgg agg, long windowSize) {
        Candle candle = agg.snapshot(state.symbol, windowSize);
        state.events.add(new CursorEvent(state.nextSeq++, type, candle));
    }

    private IngestResult result(IngestStatus status, String reason, Long windowStart, TradeTick tick) {
        log.info("[INGEST] tick={} -> {} | 判定依据: {}", tick, status, reason);
        return new IngestResult(status, reason, windowStart, tick);
    }

    /**
     * 查询区间 [fromWindowStart, toWindowStart] 内的窗口快照（含缺口）；
     * 参数为 null 表示开放边界。未观测到的未来窗口不会被臆造。
     */
    public List<Candle> snapshots(String symbol, Long fromWindowStart, Long toWindowStart) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return List.of();
        }
        synchronized (state) {
            List<Candle> out = new ArrayList<>();
            for (SymbolState.WindowAgg agg : state.windows.values()) {
                if (fromWindowStart != null && agg.windowStart < fromWindowStart) {
                    continue;
                }
                if (toWindowStart != null && agg.windowStart > toWindowStart) {
                    continue;
                }
                out.add(agg.snapshot(symbol, size()));
            }
            return out;
        }
    }

    /**
     * 读取 seq 严格大于 afterSeq 的事件，最多 limit 条，保持追加顺序。
     *
     * <p>仅用于运维/测试对事件日志的原始检视，<b>不做压缩水位判定</b>；
     * 面向客户端的增量拉取必须走 {@link #pullEvents(String, long, long, int)}，
     * 以保证“游标之后有事件被折叠”时被明确拒绝而非静默返回改写后的序列。
     */
    public List<CursorEvent> readEvents(String symbol, long afterSeq, int limit) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return List.of();
        }
        synchronized (state) {
            List<CursorEvent> out = new ArrayList<>();
            for (CursorEvent event : state.events) {
                if (event.seq() > afterSeq) {
                    out.add(event);
                    if (out.size() >= limit) {
                        break;
                    }
                }
            }
            return out;
        }
    }

    /**
     * 基于光标的增量拉取在存储层的原子结果。
     *
     * <p>必须在同一个 {@code synchronized(state)} 临界区内同时完成“压缩水位判定”
     * 与“事件读取”：否则判定通过后、读取前若恰好发生一次压缩，客户端仍会静默读到
     * 被改写过的序列。
     *
     * @param events    正常拉取时：seq 严格大于 afterSeq 的事件页（最多 limit 条）
     * @param compacted 正常拉取时为 null；被压缩拒绝时携带恢复所需的 seq 信息
     */
    public record PullResult(List<CursorEvent> events, CompactedRecovery compacted) {

        boolean ok() {
            return compacted == null;
        }

        static PullResult page(List<CursorEvent> events) {
            return new PullResult(events, null);
        }

        static PullResult compacted(CompactedRecovery recovery) {
            return new PullResult(List.of(), recovery);
        }
    }

    /**
     * 游标续拉位置已被压缩折叠时的恢复描述（不含签名游标，由游标层签发）。
     *
     * @param cursorSeq           被拒游标携带的 seq
     * @param compactedThroughSeq 当前压缩水位（被折叠事件的最大 seq）
     * @param earliestRetainedSeq 压缩后日志中保留的最小事件 seq
     */
    public record CompactedRecovery(long cursorSeq, long compactedThroughSeq, long earliestRetainedSeq) {
    }

    /**
     * 原子地完成压缩水位判定与增量读取。
     *
     * <p>压缩后日志里“折叠前保留下来的事件”（已结算窗口的 WINDOW_SETTLED）其 seq 可能
     * <b>大于</b>压缩水位 F：折叠删除的是各窗口 OPEN/UPDATE/GAP/CORRECTION，而结算事件
     * 是在窗口结算时才追加，排在很多被删除事件之后。因此“恢复授权”不是按 seq 区间放行，
     * 而是按“该游标是否已被告知发生过压缩”放行：
     * <ul>
     *   <li>普通游标（{@code recoveryUpTo == 0}）：{@code 0 < afterSeq < F} 拒绝；
     *       {@code afterSeq == 0}（从头）或 {@code afterSeq >= F} 正常读；</li>
     *   <li>恢复游标（{@code recoveryUpTo > 0}，由服务端针对水位快照 F₀ 签发、受 HMAC 保护，
     *       客户端无法伪造）：只要当前水位没有继续前进（{@code F <= F₀}），整个重放过程
     *       都允许越过水位门，从而读到 seq 晚于 F 的保留结算事件；一旦恢复期间又发生了
     *       新的压缩（{@code F > F₀}），授权失效，重新拒绝并签发更新后的恢复游标。</li>
     * </ul>
     */
    public PullResult pullEvents(String symbol, long afterSeq, long recoveryUpTo, int limit) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return PullResult.page(List.of());
        }
        synchronized (state) {
            long frontier = state.compactedThroughSeq;
            // 需要越过水位门的前提是游标仍停在折叠区间内（afterSeq < F）。
            // 已走到 F 之后（含从头 afterSeq==0）本就正常，恢复游标是否还有授权无所谓；
            // 只有“afterSeq < F 且恢复授权覆盖当前水位（F <= F0）”才放行，
            // 若恢复期间又压缩使 F > F0，授权失效，重新拒绝并签发新恢复游标。
            boolean needsGateBypass = afterSeq > 0 && afterSeq < frontier;
            boolean authorizedRecovery = recoveryUpTo > 0 && frontier <= recoveryUpTo;
            if (needsGateBypass && !authorizedRecovery) {
                long earliest = state.events.isEmpty() ? 0L : state.events.get(0).seq();
                return PullResult.compacted(new CompactedRecovery(afterSeq, frontier, earliest));
            }
            List<CursorEvent> out = new ArrayList<>();
            for (CursorEvent event : state.events) {
                if (event.seq() > afterSeq) {
                    out.add(event);
                    if (out.size() >= limit) {
                        break;
                    }
                }
            }
            return PullResult.page(out);
        }
    }

    /**
     * 该标的当前最大事件 seq（无事件时为 0）。
     */
    public long maxSeq(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.nextSeq - 1;
        }
    }

    /**
     * 压缩事件日志：折叠已结算窗口的历史事件。
     *
     * <p>规则：每个已结算窗口只保留它的 {@link EventType#WINDOW_SETTLED} 事件
     * （该事件携带窗口最终快照，且恒为该窗口的最后一条事件——结算后任何迟到数据
     * 都会被拒绝，不可能再有新事件）；未结算窗口的事件一律不动。
     * 被保留事件的 seq 不重编号，新事件接着原序号递增，绝不复用。
     *
     * <p><b>游标兼容范围（唯一判定，不再“静默放行”）：</b>
     * 本次折叠把“被删除事件的最大 seq”推进为压缩水位 {@code compactedThroughSeq}（只增不减）。
     * 客户端游标携带 seq = c：
     * <ul>
     *   <li>{@code c >= compactedThroughSeq}：不存在“seq 大于 c 的被折叠事件”，
     *       继续拉取与未压缩时逐条一致（c 本身指向已消费过的被折叠事件也没关系，
     *       它不在续拉区间内）；</li>
     *   <li>{@code 0 < c < compactedThroughSeq}：游标续拉位置之后存在被折叠事件，
     *       无法再保证与未压缩时逐条一致，{@link #pullEvents} 在同一把锁内原子判定并返回
     *       {@link CompactedRecovery}，由上层明确拒绝并给出恢复起点，
     *       绝不返回被改写过的序列让客户端误以为拿到了全部；</li>
     *   <li>{@code c == 0}（首次从头拉取）：永远可用——每个已结算窗口都保留其
     *       WINDOW_SETTLED（最终快照），从头重放仍能重建全部窗口的最终状态。</li>
     * </ul>
     * 在与接入相同的 {@code synchronized(state)} 临界区内执行，压缩与读写不会交错。
     * 幂等：对已压缩过的日志再次执行不删除任何事件，压缩水位不变。
     */
    public CompactionReport compact(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return new CompactionReport(symbol, 0, 0, 0, 0L);
        }
        synchronized (state) {
            List<CursorEvent> kept = new ArrayList<>(state.events.size());
            int dropped = 0;
            long maxDroppedSeq = 0L;
            int settledWindows = 0;
            for (CursorEvent event : state.events) {
                SymbolState.WindowAgg agg = state.windows.get(event.candle().windowStart());
                boolean collapsible = agg != null && agg.settled
                        && event.type() != EventType.WINDOW_SETTLED;
                if (collapsible) {
                    dropped++;
                    maxDroppedSeq = Math.max(maxDroppedSeq, event.seq());
                } else {
                    kept.add(event);
                }
            }
            for (SymbolState.WindowAgg agg : state.windows.values()) {
                if (agg.settled) {
                    settledWindows++;
                }
            }
            state.events.clear();
            state.events.addAll(kept);
            // 压缩水位只增不减：本次若折叠了新结算窗口的事件，水位向前推进。
            // 据此可在拉取侧识别“游标之后有事件被折叠”的旧游标并明确拒绝。
            state.compactedThroughSeq = Math.max(state.compactedThroughSeq, maxDroppedSeq);
            CompactionReport report = new CompactionReport(
                    symbol, settledWindows, dropped, kept.size(), maxDroppedSeq);
            log.info("[COMPACT] {}", report);
            return report;
        }
    }

    /**
     * 该标的的压缩水位：被折叠删除事件的最大 seq（从未压缩过为 0）。
     * 在 {@code synchronized(state)} 内读取，与拉取/压缩使用同一临界区。
     */
    public long compactedThroughSeq(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.compactedThroughSeq;
        }
    }

    /**
     * 压缩后事件日志中仍保留的最小事件 seq；日志为空（无事件或全部被折叠，
     * 实际不会发生——每个已结算窗口都保留其结算事件）时返回 0。
     */
    public long earliestRetainedSeq(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.events.isEmpty() ? 0L : state.events.get(0).seq();
        }
    }
}
