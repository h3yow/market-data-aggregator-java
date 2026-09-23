package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * 单个标的的全部内存状态。
 *
 * <p>持有：
 * <ul>
 *   <li>{@code windows}：按窗口起点排序的窗口聚合表（含已标记的缺口窗口）；</li>
 *   <li>{@code seenTradeIds}：已接受成交 ID 集合，构成 (symbol, tradeId) 幂等索引；</li>
 *   <li>{@code watermark}：已接受行情的最大时间戳，用于倒退/乱序判定；</li>
 *   <li>{@code maxWindowStart}：已观察到的最大窗口起点，水位跨窗口推进时据此找缺口；</li>
 *   <li>{@code events}：只追加的游标事件日志，{@code seq} 单调递增。</li>
 * </ul>
 *
 * <p>本对象自身不加锁；所有访问由 {@link AggregationStore} 在
 * {@code synchronized(state)} 块内串行化，不同标的使用不同监视器可并行。
 */
public final class SymbolState {

    public final String symbol;
    public final TreeMap<Long, WindowAgg> windows = new TreeMap<>();
    public final Set<Long> seenTradeIds = new HashSet<>();
    public long watermark = Long.MIN_VALUE;
    public Long maxWindowStart = null;
    public final List<CursorEvent> events = new ArrayList<>();
    public long nextSeq = 1L;

    public SymbolState(String symbol) {
        this.symbol = symbol;
    }

    /**
     * 可变的单窗口聚合。
     *
     * <p>开盘/收盘的时间戳平局规则唯一确定：open 取窗口内“最早时间戳”的成交价，
     * 多笔共享最早时间戳时以先接受者为准；close 取“最晚时间戳”的成交价，
     * 多笔共享最晚时间戳时以后接受者为准（同一标的的接入被串行化，接受顺序即确定顺序）。
     * 因此窗口内乱序到达只需比较时间戳即可归位，结果与到达顺序无关，仅在时间戳完全相同
     * 时按接受顺序打破平局。
     */
    public static final class WindowAgg {
        public final long windowStart;
        public long firstTimestamp = Long.MAX_VALUE;   // 窗口内最小时间戳（定 open）
        public long lastTimestamp = Long.MIN_VALUE;    // 窗口内最大时间戳（定 close）
        public double open;
        public double high;
        public double low;
        public double close;
        public long volume;
        public long tickCount;
        public boolean gap = true;
        public boolean corrected = false;
        public long version = 1;

        public WindowAgg(long windowStart) {
            this.windowStart = windowStart;
        }

        /** 并入一笔成交；幂等由调用方保证，同一笔不会进入两次。 */
        public void apply(long timestamp, double price, long tickVolume) {
            boolean first = tickCount == 0;
            tickCount++;
            gap = false;
            volume += tickVolume;
            if (first) {
                open = high = low = close = price;
                firstTimestamp = lastTimestamp = timestamp;
                return;
            }
            if (timestamp < firstTimestamp) {
                firstTimestamp = timestamp;
                open = price;
            }
            // 注意用 >=：同时间戳平局由后接受者胜出，决定收盘价
            if (timestamp >= lastTimestamp) {
                lastTimestamp = timestamp;
                close = price;
            }
            high = Math.max(high, price);
            low = Math.min(low, price);
        }

        public Candle snapshot(String sym, long windowSizeMillis) {
            if (gap) {
                return new Candle(sym, windowStart, windowStart + windowSizeMillis,
                        null, null, null, null, 0L, true, false, version);
            }
            return new Candle(sym, windowStart, windowStart + windowSizeMillis,
                    open, high, low, close, volume, false, corrected, version);
        }
    }
}
