package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * K 线快照查询。
 *
 * <p>入参为任意毫秒时间戳，统一按 {@link WindowClock} 向下取整到窗口起点，
 * 因此“按时间查”与“按窗口查”的边界唯一确定。返回区间内所有已存在的窗口，
 * 包括被标记为缺口（gap=true）的窗口；从未观测到的未来窗口不会被臆造。
 */
@Service
public class QueryService {

    private final AggregationStore store;
    private final MarketDataProperties properties;

    public QueryService(AggregationStore store, MarketDataProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * @param fromMillis 起始时间（含），null 表示从最早窗口开始；向下取整到窗口起点
     * @param toMillis   结束时间（含所在窗口），null 表示到最新窗口；向下取整到窗口起点
     */
    public List<Candle> queryCandles(String symbol, Long fromMillis, Long toMillis) {
        long size = properties.getWindowSizeMillis();
        Long fromWindowStart = fromMillis == null ? null : WindowClock.windowStart(fromMillis, size);
        Long toWindowStart = toMillis == null ? null : WindowClock.windowStart(toMillis, size);
        return store.snapshots(symbol, fromWindowStart, toWindowStart);
    }
}
