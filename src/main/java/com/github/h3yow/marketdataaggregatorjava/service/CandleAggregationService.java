package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.model.Candle;
import com.github.h3yow.marketdataaggregatorjava.model.CandleChange;
import com.github.h3yow.marketdataaggregatorjava.model.PullResponse;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import com.github.h3yow.marketdataaggregatorjava.model.WindowStatus;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * K线聚合与缺口管理。
 *
 * <p>每个标的独立加锁，锁内完成：去重后的行情写入、窗口物化、缺口标记与变更日志追加。
 * 窗口内的OHLCV由该窗口已接受的全部行情按 (成交时间, tradeId) 确定性重算，
 * 因此乱序/迟到回补只会修正结果，不会重复累计成交量。
 */
@Service
public class CandleAggregationService {

    public enum ApplyOutcome {
        /** 最新前沿上的新窗口 */
        NEW_WINDOW,
        /** 最新前沿窗口再次更新 */
        UPDATED,
        /** 回补/修正一个已有数据但已越过的旧窗口 */
        LATE_FILL,
        /** 回补一个此前被标记为缺口的窗口 */
        BACKFILLED,
        /** tradeId 已处理过（幂等拒绝） */
        DUPLICATE
    }

    public static final class ApplyResult {
        private final ApplyOutcome outcome;
        private final long windowStart;
        private final long changeSeq;

        public ApplyResult(ApplyOutcome outcome, long windowStart, long changeSeq) {
            this.outcome = outcome;
            this.windowStart = windowStart;
            this.changeSeq = changeSeq;
        }

        public ApplyOutcome getOutcome() { return outcome; }
        public long getWindowStart() { return windowStart; }
        public long getChangeSeq() { return changeSeq; }
    }

    /** 窗口内部可变状态。 */
    private static final class WindowData {
        final List<Tick> ticks = new ArrayList<>();
        Candle snapshot;
    }

    private static final class SymbolState {
        final NavigableMap<Long, WindowData> windows = new TreeMap<>();
        final List<CandleChange> changelog = new ArrayList<>();
        final Set<String> knownTradeIds = new HashSet<>();
        long frontier = Long.MIN_VALUE;
        long nextSeq = 1;
    }

    private final AggregatorProperties properties;
    private final CursorCodec cursorCodec;
    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public CandleAggregationService(AggregatorProperties properties, CursorCodec cursorCodec) {
        this.properties = properties;
        this.cursorCodec = cursorCodec;
    }

    @PostConstruct
    void validateConfig() {
        if (properties.getWindowMs() <= 0) {
            throw new IllegalStateException("market-data.window-ms must be positive");
        }
        if (properties.getMaxLateMs() < 0) {
            throw new IllegalStateException("market-data.max-late-ms must not be negative");
        }
        if (properties.getCursorTtlMs() <= 0) {
            throw new IllegalStateException("market-data.cursor-ttl-ms must be positive");
        }
        if (properties.getPullBatchSize() <= 0) {
            throw new IllegalStateException("market-data.pull-batch-size must be positive");
        }
    }

    public long windowStartOf(long tradeTime) {
        return Math.floorDiv(tradeTime, properties.getWindowMs()) * properties.getWindowMs();
    }

    /**
     * 应用一笔已通过接入层校验、且 tradeId 未出现过的行情。
     * 同一标的串行化；窗口OHLCV整体重算。
     */
    public ApplyResult applyTick(Tick tick) {
        long w = windowStartOf(tick.getTradeTime());
        Object lock = locks.computeIfAbsent(tick.getSymbol(), k -> new Object());
        synchronized (lock) {
            SymbolState state = states.computeIfAbsent(tick.getSymbol(), k -> new SymbolState());
            // 幂等去重与写入必须在同一临界区，避免"检查通过但尚未写入"的竞态
            if (!state.knownTradeIds.add(tick.getTradeId())) {
                return new ApplyResult(ApplyOutcome.DUPLICATE, w, -1L);
            }
            WindowData data;
            ApplyOutcome outcome;
            boolean isFirstEver = state.windows.isEmpty();
            boolean newWindow = !state.windows.containsKey(w);

            if (newWindow) {
                if (!isFirstEver && w > state.frontier) {
                    // 前沿向前推进：首个窗口与新前沿之间的空窗口物化为缺口
                    for (long g = state.frontier + properties.getWindowMs(); g < w;
                         g += properties.getWindowMs()) {
                        WindowData gap = new WindowData();
                        gap.snapshot = buildGapCandle(tick.getSymbol(), g);
                        state.windows.put(g, gap);
                        appendChange(state, g, WindowStatus.GAP, gap.snapshot);
                    }
                }
                data = new WindowData();
                state.windows.put(w, data);

                long oldFrontier = state.frontier;
                state.frontier = Math.max(oldFrontier, w);
                if (!isFirstEver && w < oldFrontier) {
                    // 迟到数据落到前沿之前、此前从未出现的窗口：作为迟到修正
                    outcome = ApplyOutcome.LATE_FILL;
                } else {
                    outcome = ApplyOutcome.NEW_WINDOW;
                }
            } else {
                data = state.windows.get(w);
                WindowStatus existing = data.snapshot == null ? null : data.snapshot.getStatus();
                if (existing == WindowStatus.GAP || existing == WindowStatus.BACKFILLED) {
                    outcome = ApplyOutcome.BACKFILLED;
                } else if (w < state.frontier) {
                    outcome = ApplyOutcome.LATE_FILL;
                } else {
                    outcome = ApplyOutcome.UPDATED;
                }
            }

            data.ticks.add(tick);
            data.snapshot = recompute(tick.getSymbol(), w, data,
                    outcome == ApplyOutcome.BACKFILLED ? WindowStatus.BACKFILLED : WindowStatus.FILLED);
            appendChange(state, w, data.snapshot.getStatus(), data.snapshot);
            return new ApplyResult(outcome, w, state.changelog.get(state.changelog.size() - 1).getSeq());
        }
    }

    /** tradeId 是否已在该标的处理过。真正的原子去重发生在 {@link #applyTick} 同一临界区内。 */
    public boolean isKnownTrade(String symbol, String tradeId) {
        Object lock = locks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            SymbolState state = states.get(symbol);
            return state != null && state.knownTradeIds.contains(tradeId);
        }
    }

    /** 返回某窗口K线快照（含 GAP），不存在返回 null。 */
    public Candle getCandle(String symbol, long windowStart) {
        Object lock = locks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            SymbolState state = states.get(symbol);
            if (state == null) {
                return null;
            }
            WindowData data = state.windows.get(windowStart);
            return data == null || data.snapshot == null ? null : copy(data.snapshot);
        }
    }

    public WindowStatus windowStatus(String symbol, long windowStart) {
        Candle c = getCandle(symbol, windowStart);
        return c == null ? null : c.getStatus();
    }

    /** 测试/排障用：读取变更日志副本。 */
    public List<CandleChange> debugChangelog(String symbol) {
        Object lock = locks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            SymbolState state = states.get(symbol);
            if (state == null) {
                return List.of();
            }
            List<CandleChange> out = new ArrayList<>(state.changelog.size());
            for (CandleChange ch : state.changelog) {
                out.add(copyChange(ch));
            }
            return out;
        }
    }

    /**
     * 基于游标的增量拉取：返回 seq 严格大于游标 seq 的变更，重复拉取不重不漏。
     */
    public PullResponse pull(String symbol, String cursorToken, long now) {
        Object lock = locks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            SymbolState state = states.computeIfAbsent(symbol, k -> new SymbolState());
            long afterSeq = 0L;
            if (cursorToken != null && !cursorToken.isBlank()) {
                CursorCodec.Cursor cursor = cursorCodec.decode(cursorToken, symbol, now);
                afterSeq = cursor.getSeq();
            }
            int batch = properties.getPullBatchSize();
            List<CandleChange> page = new ArrayList<>();
            long lastSeq = afterSeq;
            int taken = 0;
            for (CandleChange ch : state.changelog) {
                if (ch.getSeq() <= afterSeq) {
                    continue;
                }
                if (taken >= batch) {
                    break;
                }
                page.add(copyChange(ch));
                lastSeq = ch.getSeq();
                taken++;
            }
            boolean hasMore = false;
            for (CandleChange ch : state.changelog) {
                if (ch.getSeq() > lastSeq) {
                    hasMore = true;
                    break;
                }
            }
            PullResponse resp = new PullResponse();
            resp.setSymbol(symbol);
            resp.setChanges(page);
            resp.setHasMore(hasMore);
            resp.setNextCursor(cursorCodec.encode(symbol, lastSeq, now));
            return resp;
        }
    }

    // ---- internals ------------------------------------------------------

    private void appendChange(SymbolState state, long windowStart, WindowStatus status, Candle candle) {
        CandleChange change = new CandleChange();
        change.setSeq(state.nextSeq++);
        change.setWindowStart(windowStart);
        change.setStatus(status);
        change.setCandle(copy(candle));
        state.changelog.add(change);
    }

    /**
     * 由窗口内全部已接受行情确定性重算OHLCV：
     * open/收盘按 (成交时间, tradeId) 升序取首尾；成交量逐条求和。
     */
    private Candle recompute(String symbol, long windowStart, WindowData data, WindowStatus status) {
        List<Tick> ticks = new ArrayList<>(data.ticks);
        ticks.sort((a, b) -> {
            int cmp = Long.compare(a.getTradeTime(), b.getTradeTime());
            if (cmp != 0) {
                return cmp;
            }
            return a.getTradeId().compareTo(b.getTradeId());
        });
        Tick first = ticks.get(0);
        Tick last = ticks.get(ticks.size() - 1);
        long high = Long.MIN_VALUE;
        long low = Long.MAX_VALUE;
        long volume = 0L;
        for (Tick t : ticks) {
            high = Math.max(high, t.getPrice());
            low = Math.min(low, t.getPrice());
            volume += t.getVolume();
        }
        Candle c = new Candle();
        c.setSymbol(symbol);
        c.setWindowStart(windowStart);
        c.setWindowEnd(windowStart + properties.getWindowMs());
        c.setOpenPrice(first.getPrice());
        c.setHighPrice(high);
        c.setLowPrice(low);
        c.setClosePrice(last.getPrice());
        c.setVolume(volume);
        c.setFirstTickTime(first.getTradeTime());
        c.setLastTickTime(last.getTradeTime());
        c.setStatus(status);
        return c;
    }

    private Candle buildGapCandle(String symbol, long windowStart) {
        Candle c = new Candle();
        c.setSymbol(symbol);
        c.setWindowStart(windowStart);
        c.setWindowEnd(windowStart + properties.getWindowMs());
        c.setStatus(WindowStatus.GAP);
        return c;
    }

    private Candle copy(Candle c) {
        Candle copy = new Candle();
        copy.setSymbol(c.getSymbol());
        copy.setWindowStart(c.getWindowStart());
        copy.setWindowEnd(c.getWindowEnd());
        copy.setOpenPrice(c.getOpenPrice());
        copy.setHighPrice(c.getHighPrice());
        copy.setLowPrice(c.getLowPrice());
        copy.setClosePrice(c.getClosePrice());
        copy.setVolume(c.getVolume());
        copy.setFirstTickTime(c.getFirstTickTime());
        copy.setLastTickTime(c.getLastTickTime());
        copy.setStatus(c.getStatus());
        copy.setUpdateSeq(c.getUpdateSeq());
        return copy;
    }

    private CandleChange copyChange(CandleChange ch) {
        CandleChange copy = new CandleChange();
        copy.setSeq(ch.getSeq());
        copy.setWindowStart(ch.getWindowStart());
        copy.setStatus(ch.getStatus());
        copy.setCandle(ch.getCandle() == null ? null : copy(ch.getCandle()));
        return copy;
    }
}
