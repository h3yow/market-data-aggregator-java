package com.github.h3yow.marketdataaggregatorjava.tests.gap;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.EventType;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestStatus;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 无行情窗口的缺口标记、迟到数据回补修正、回补幂等性测试。
 */
class GapBackfillTest {

    private static final Logger log = LoggerFactory.getLogger(GapBackfillTest.class);

    private static final String SYM = "CU2312";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;

    private AggregationStore store;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(60_000);
        store = new AggregationStore(props);
        ingestService = new IngestService(store);
    }

    private IngestResult send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        IngestResult r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}", r.status(),
                r.windowStart(), r.reason());
        return r;
    }

    private Map<Long, Candle> candles() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    @Test
    @DisplayName("水位跳过两个窗口：中间窗口被标记为 gap=true，OHLC 为 null，量为 0")
    void skippedWindowsAreMarkedAsGaps() {
        send(1, 100, 10.0, 1);      // w0
        send(2, 3500, 20.0, 1);     // w3，w1/w2 应为缺口

        Map<Long, Candle> m = candles();
        Candle g1 = m.get(T0 + 1000);
        Candle g2 = m.get(T0 + 2000);
        log.info("[TEST-ASSERT] gap w1={}, gap w2={}", g1, g2);
        assertTrue(g1.gap());
        assertTrue(g2.gap());
        assertNull(g1.open());
        assertNull(g1.high());
        assertNull(g1.low());
        assertNull(g1.close());
        assertEquals(0L, g1.volume());
        assertFalse(m.get(T0).gap());
        assertFalse(m.get(T0 + 3000).gap());
    }

    @Test
    @DisplayName("迟到数据回补缺口窗口：缺口转正式K线、corrected=true、version 递增，OHLCV 正确")
    void lateTickBackfillsGapAndBumpsVersion() {
        send(1, 100, 10.0, 1);
        send(2, 3500, 20.0, 1);
        Candle gapBefore = candles().get(T0 + 1000);
        assertEquals(1L, gapBefore.version());

        IngestResult backfill = send(3, 1200, 10.5, 7); // 回补 w1
        assertEquals(IngestStatus.ACCEPTED_LATE, backfill.status());
        Candle filled = candles().get(T0 + 1000);
        log.info("[TEST-ASSERT] 回补后窗口={}", filled);
        assertFalse(filled.gap());
        assertTrue(filled.corrected());
        assertEquals(2L, filled.version());
        assertEquals(10.5, filled.open(), 1e-9);
        assertEquals(10.5, filled.close(), 1e-9);
        assertEquals(7L, filled.volume());
    }

    @Test
    @DisplayName("回补同一窗口的第二笔迟到数据：再次修正，version=3，成交量累加而非重复")
    void secondLateTickAccumulatesAndBumpsVersionAgain() {
        send(1, 100, 10.0, 1);
        send(2, 3500, 20.0, 1);
        send(3, 1200, 10.5, 7);
        send(4, 1300, 10.8, 3); // 同一缺口窗口第二笔

        Candle filled = candles().get(T0 + 1000);
        assertEquals(3L, filled.version());
        assertEquals(10L, filled.volume());
        assertEquals(10.5, filled.open(), 1e-9);
        assertEquals(10.8, filled.close(), 1e-9);
        assertEquals(10.8, filled.high(), 1e-9);
        assertEquals(10.5, filled.low(), 1e-9);
    }

    @Test
    @DisplayName("回补幂等：回补笔的重复推送判 DUPLICATE，窗口不重复累计、版本不变")
    void backfillTickDuplicateDoesNotDoubleCount() {
        send(1, 100, 10.0, 1);
        send(2, 3500, 20.0, 1);
        send(3, 1200, 10.5, 7);
        IngestResult dup = send(3, 1200, 10.5, 7);

        assertEquals(IngestStatus.DUPLICATE, dup.status());
        Candle filled = candles().get(T0 + 1000);
        assertEquals(7L, filled.volume());
        assertEquals(2L, filled.version());
    }

    @Test
    @DisplayName("事件流顺序确定：WINDOW_OPEN -> GAP_MARKED -> CORRECTION")
    void eventOrderIsWindowOpenGapCorrection() {
        send(1, 100, 10.0, 1);
        send(2, 3500, 20.0, 1);
        send(3, 1200, 10.5, 7); // 回补 w1

        List<CursorEvent> events = store.readEvents(SYM, 0, 100);
        log.info("[TEST-ASSERT] 事件序列={}",
                events.stream().map(e -> e.seq() + ":" + e.type()).toList());
        assertEquals(EventType.WINDOW_OPEN, events.get(0).type());   // w0
        assertEquals(EventType.GAP_MARKED, events.get(1).type());    // w1 缺口
        assertEquals(EventType.GAP_MARKED, events.get(2).type());    // w2 缺口
        assertEquals(EventType.WINDOW_OPEN, events.get(3).type());   // w3
        assertEquals(EventType.CORRECTION, events.get(4).type());    // w1 回补
        assertEquals(T0 + 1000, events.get(4).candle().windowStart());
        assertFalse(events.get(4).candle().gap());
    }

        @Test
    @DisplayName("超过容忍的迟到回补被拒绝：缺口保持缺口，不被污染")
    void staleBackfillIsRejectedAndGapRemains() {
        send(1, 100, 10.0, 1);
        send(2, 90_000, 20.0, 1); // 水位推到 90s 后，中间全是缺口
        IngestResult stale = send(3, 1200, 10.5, 1); // 倒退 88800ms，超过 60s 容忍
        assertEquals(IngestStatus.REJECTED_STALE, stale.status());
        Candle g1 = candles().get(T0 + 1000);
        assertTrue(g1.gap());
        assertEquals(0L, g1.volume());
    }
}
