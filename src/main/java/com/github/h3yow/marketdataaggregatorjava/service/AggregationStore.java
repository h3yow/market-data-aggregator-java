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
     * 被保留事件的 seq 不重编号，并记录本标的“压缩前沿” {@code compactedThroughSeq}
     * （历次被删除事件 seq 的高水位，单调不减）。
     *
     * <p>游标兼容范围（同一游标压缩前后看到的内容）：
     * <ul>
     *   <li><b>cursor.seq &gt;= compactedThroughSeq</b>：该位置之后的事件一条未删，
     *       续拉序列与未压缩时<b>逐条一致</b>（不重不漏）；</li>
     *   <li><b>cursor.seq &lt; compactedThroughSeq</b>：续拉位置所在事件已被删除，
     *       无法再保证逐条一致，{@code pull} 明确以
     *       {@link CursorException.Reason#COMPACTED} 拒绝并返回恢复起点（seq=0、
     *       新游标），绝不静默返回对不上的半截序列。客户端改用新游标从头重放：
     *       已结算窗口各自保留一条结算事件（最终快照），未结算窗口事件未动，
     *       重建出的每个窗口状态与区间查询完全一致。</li>
     * </ul>
     * 在与接入相同的 {@code synchronized(state)} 临界区内执行，压缩与读写不会交错。
     * 幂等：对已压缩过的日志再次执行不删除任何事件、压缩前沿不回退。
     */
    public CompactionReport compact(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return new CompactionReport(symbol, 0, 0, 0, 0L, 0L);
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
            // 记录压缩前沿（单调不减）：续拉位置落在该 seq 之前（严格小于）的游标
            // 已无法逐条一致，必须显式拒绝并引导恢复；重复压缩不回退该值。
            state.compactedThroughSeq = Math.max(state.compactedThroughSeq, maxDroppedSeq);
            CompactionReport report = new CompactionReport(
                    symbol, settledWindows, dropped, kept.size(), maxDroppedSeq,
                    state.compactedThroughSeq);
            log.info("[COMPACT] {}", report);
            return report;
        }
    }

    /**
     * 该标的的压缩前沿（历次压缩被删除事件 seq 的高水位，从未压缩为 0）。
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
     * 当前日志中最早一条仍保留事件的 seq（空日志为 0）。
     * 压缩的无损恢复起点即该 seq（不含）：从此处重放恰好遍历全部保留事件，
     * 每个已结算窗口的 WINDOW_SETTLED 最终快照都在其中。
     * 注意保留事件的 seq 可能小于压缩前沿（更早窗口的结算事件），
     * 因此不能用 frontier 本身作为恢复序号。
     */
    public long firstRetainedSeq(String symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.events.isEmpty() ? 0L : state.events.get(0).seq();
        }
    }

    /**
     * 基于游标的增量拉取的判定与读取结果（同一把锁内完成，杜绝
     * “先判位置、后压缩、再读”的 TOCTOU 错配）。
     *
     * <p>两类游标承诺不同：
     * <ul>
     *   <li><b>常规游标（recovery=false）</b>承诺逐条一致：其后只要有被删事件
     *       （{@code seq < compactedThroughSeq}）即拒绝，由调用方返回恢复起点；</li>
     *   <li><b>恢复游标（recovery=true）</b>只承诺最终视图一致：其后有被删事件
     *       也不拒绝；若位置已落在“整段更老的、不再保留任何事件”的区间
     *       （{@code seq < 首条保留事件-1}），则在锁内把读取位置透明快进到
     *       当前日志开头后读取。跳过的都是更老已结算窗口的中间态，其
     *       WINDOW_SETTLED 最终快照仍在保留日志内，因此视图无损、seq 只向前，
     *       且不会在压缩持续推进时陷入“反复重置”活锁。</li>
     * </ul>
     *
     * @param recovery 是否压缩恢复游标（4 段 payload）；false 为常规游标
     */
    public PullOutcome pullFrom(String symbol, long afterSeq, boolean recovery, int limit) {
        SymbolState state = states.get(symbol);
        if (state == null) {
            return new PullOutcome(0L, 0L, List.of(), 0L, false);
        }
        synchronized (state) {
            long firstRetained = state.events.isEmpty() ? 0L : state.events.get(0).seq();
            if (!recovery) {
                // 常规游标承诺逐条一致：其后只要存在被删事件（seq < 前沿）即拒绝
                if (afterSeq < state.compactedThroughSeq) {
                    return new PullOutcome(afterSeq, state.compactedThroughSeq,
                            List.of(), firstRetained, true);
                }
            } else if (afterSeq < firstRetained - 1) {
                // 恢复游标只承诺最终视图：位置落在“已整段删除的更老历史”时
                // 透明快进到当前保留日志开头——被跳过的都是更老已结算窗口，
                // 其 WINDOW_SETTLED 最终快照仍在保留日志内，视图无损、
                // seq 只向前，且不会在压缩推进时反复重置（活锁）。
                afterSeq = Math.max(0L, firstRetained - 1);
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
            return new PullOutcome(afterSeq, state.compactedThroughSeq, List.copyOf(out), firstRetained, false);
        }
    }

    /**
     * 拉取原子结果。{@code rejected} 为 true 时 {@code events} 必为空，
     * {@code resumeHead} 是可用的恢复起点（afterSeq 语义）。
     */
    public record PullOutcome(long afterSeq, long compactedThroughSeq, List<CursorEvent> events,
                              long firstRetainedSeq, boolean rejected) {
        /** 恢复起点（首条保留事件 seq 之前；空日志为 0）。 */
        public long resumeHead() {
            return Math.max(0L, firstRetainedSeq - 1);
        }
    }
}
