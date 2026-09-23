package com.github.h3yow.marketdataaggregatorjava.tests.aggregation;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestStatus;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import com.github.h3yow.marketdataaggregatorjava.service.MarketDataService;
import com.github.h3yow.marketdataaggregatorjava.service.QueryService;
import com.github.h3yow.marketdataaggregatorjava.service.impl.MarketDataServiceImpl;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 固定窗口聚合测试：窗口边界归属唯一、窗口内乱序到达时 OHLCV 与到达顺序无关、
 * 跨窗口不串数据。
 */
class AggregationWindowTest {

    private static final Logger log = LoggerFactory.getLogger(AggregationWindowTest.class);

    private static final String SYM = "IF2312";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;

    private AggregationStore store;
    private IngestService ingestService;
    private QueryService queryService;
    private MarketDataService marketDataService;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(60_000);
        store = new AggregationStore(props);
        ingestService = new IngestService(store);
        queryService = new QueryService(store, props);
        marketDataService = new MarketDataServiceImpl(ingestService, queryService,
                new CursorService(store, props));
    }

    private void send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        var r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}",
                r.status(), r.windowStart(), r.reason());
    }

    private Map<Long, Candle> byWindow(List<Candle> candles) {
        return candles.stream().collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    @Test
    @DisplayName("边界归属唯一：[start,start+size) 左闭右开；start+size 整秒点归入下一窗口")
    void windowBoundaryIsLeftClosedRightOpen() {
        send(1, 0, 10.0, 1);       // T0 属于 w0
        send(2, 999, 10.0, 1);     // 仍属 w0
        send(3, 1000, 20.0, 1);    // 整点属于 w1
        send(4, -1000, 30.0, 1);   // 迟到，属于 w-1（对负偏移取整无歧义）

        Map<Long, Candle> m = byWindow(queryService.queryCandles(SYM, null, null));
        assertEquals(2L, m.get(T0).volume());
        assertEquals(1L, m.get(T0 + 1000).volume());
        assertEquals(1L, m.get(T0 - 1000).volume());
        assertEquals(T0 + 1000, m.get(T0).windowEnd());
    }

    @Test
    @DisplayName("窗口内乱序到达：开盘看最早时间戳、收盘看最晚时间戳，结果与到达顺序无关")
    void outOfOrderWithinWindowProducesDeterministicOhlc() {
        // 故意按 乱序 顺序发送
        send(3, 300, 9.0, 1);
        send(1, 100, 10.0, 2);
        send(4, 400, 9.5, 1);
        send(2, 200, 11.0, 3);

        Candle w = byWindow(queryService.queryCandles(SYM, null, null)).get(T0);
        log.info("[TEST-ASSERT] 聚合结果 open={} high={} low={} close={} volume={}",
                w.open(), w.high(), w.low(), w.close(), w.volume());
        assertEquals(10.0, w.open(), 0.000001);   // ts=100 最早
        assertEquals(11.0, w.high(), 0.000001);
        assertEquals(9.0, w.low(), 0.000001);
        assertEquals(9.5, w.close(), 0.000001);   // ts=400 最晚
        assertEquals(7L, w.volume());
        assertFalse(w.gap());
    }

    @Test
    @DisplayName("与乱序结果一致：同样4笔按时间正序发送，OHLCV 必须完全相同")
    void inOrderAndOutOfOrderYieldSameResult() {
        send(11, 100, 10.0, 2);
        send(12, 200, 11.0, 3);
        send(13, 300, 9.0, 1);
        send(14, 400, 9.5, 1);
        Candle w = byWindow(queryService.queryCandles(SYM, null, null)).get(T0);
        assertEquals(10.0, w.open(), 1e-9);
        assertEquals(11.0, w.high(), 1e-9);
        assertEquals(9.0, w.low(), 1e-9);
        assertEquals(9.5, w.close(), 1e-9);
        assertEquals(7L, w.volume());
    }

    @Test
    @DisplayName("跨窗口：相邻窗口各自聚合，成交量不串窗口；重复推送不改变任何窗口")
    void adjacentWindowsAggregateIndependently() {
        send(1, 100, 10.0, 4);
        send(2, 1200, 20.0, 6);
        send(1, 100, 10.0, 4); // 重复
        Map<Long, Candle> m = byWindow(queryService.queryCandles(SYM, null, null));
        assertEquals(4L, m.get(T0).volume());
        assertEquals(6L, m.get(T0 + 1000).volume());
        assertEquals(10.0, m.get(T0).open(), 1e-9);
        assertEquals(20.0, m.get(T0 + 1000).open(), 1e-9);
    }

    @Test
    @DisplayName("同时间戳多笔：开盘=首笔接受价，收盘=最晚接受笔，成交量累加")
    void sameTimestampTicksAccumulateVolume() {
        send(1, 500, 10.0, 1);
        send(2, 500, 11.0, 2);
        send(3, 500, 9.0, 3);
        Candle w = byWindow(queryService.queryCandles(SYM, null, null)).get(T0);
        assertEquals(6L, w.volume());
        assertEquals(11.0, w.high(), 1e-9);
        assertEquals(9.0, w.low(), 1e-9);
        // 同时间戳时先到先定 open，最后到的定 close（应用层顺序串行化，确定可解释）
        assertEquals(10.0, w.open(), 1e-9);
        assertEquals(9.0, w.close(), 1e-9);
    }
}
