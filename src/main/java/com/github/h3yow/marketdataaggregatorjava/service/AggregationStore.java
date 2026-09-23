package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
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
            // 仍在当前开放窗口内：更新快照，不产生新的游标事件（开放窗口的实时值走快照查询）
            SymbolState.WindowAgg agg = state.windows.get(ws);
            agg.apply(tick.timestamp(), tick.price(), tick.volume());
            state.seenTradeIds.add(tick.tradeId());
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
        long gapCount = (ws - prevMax) / windowSize - 1;
        return result(IngestStatus.ACCEPTED,
                "水位推进至窗口 " + ws + "，中间标记缺口窗口 " + gapCount + " 个，新窗口开窗",
                ws, tick);
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
}
