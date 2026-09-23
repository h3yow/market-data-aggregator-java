package com.github.h3yow.marketdataaggregatorjava.tests;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.model.Candle;
import com.github.h3yow.marketdataaggregatorjava.model.IngestDecision;
import com.github.h3yow.marketdataaggregatorjava.model.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import com.github.h3yow.marketdataaggregatorjava.model.WindowStatus;
import com.github.h3yow.marketdataaggregatorjava.service.CandleAggregationService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorCodec;
import com.github.h3yow.marketdataaggregatorjava.service.TickIngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖：重复推送、乱序与迟到、跨窗口边界。每条断言都打印输入行情与判定依据。
 */
class TickIngestServiceTest {

    private static final Logger log = LoggerFactory.getLogger(TickIngestServiceTest.class);

    private AggregatorProperties props;
    private TickIngestService ingest;
    private CandleAggregationService agg;

    @BeforeEach
    void setUp() {
        props = new AggregatorProperties();
        props.setWindowMs(1000);
        props.setMaxLateMs(5000);
        CursorCodec codec = new CursorCodec(props);
        agg = new CandleAggregationService(props, codec);
        ingest = new TickIngestService(agg, props);
    }

    private IngestResult push(String id, long t, long price, long vol, long now) {
        Tick tick = new Tick();
        tick.setSymbol("600000");
        tick.setTradeId(id);
        tick.setPrice(price);
        tick.setVolume(vol);
        tick.setTradeTime(t);
        IngestResult r = ingest.ingest(tick, now);
        log.info("[CASE] input tick=(symbol=600000,tradeId={},price={},volume={},tradeTime={},now={}) "
                + "=> accepted={}, decision={}, windowStart={}, reason={}",
                id, price, vol, t, now, r.isAccepted(), r.getDecision(), r.getWindowStart(), r.getReason());
        return r;
    }

    @Test
    void duplicatePushIsRejectedAndNotCountedAgain() {
        IngestResult first = push("T1", 100, 100, 10, 200);
        assertTrue(first.isAccepted());
        assertEquals(IngestDecision.ACCEPTED_NEW, first.getDecision());

        // 完全相同的重复推送
        IngestResult dup = push("T1", 100, 100, 10, 300);
        assertFalse(dup.isAccepted());
        assertEquals(IngestDecision.REJECTED_DUPLICATE, dup.getDecision());

        // 即使重复推送里价格/量被篡改，仍按 tradeId 幂等拒绝，绝不污染聚合
        IngestResult dupTampered = push("T1", 100, 999, 999, 400);
        assertFalse(dupTampered.isAccepted());
        assertEquals(IngestDecision.REJECTED_DUPLICATE, dupTampered.getDecision());

        Candle c = agg.getCandle("600000", 0);
        assertNotNull(c);
        assertEquals(100, c.getOpenPrice());
        assertEquals(100, c.getClosePrice());
        assertEquals(10, c.getVolume());
        log.info("[ASSERT] after duplicates candle OHLCV={}/{}/{}/{}/{} status={}",
                c.getOpenPrice(), c.getHighPrice(), c.getLowPrice(), c.getClosePrice(),
                c.getVolume(), c.getStatus());
    }

    @Test
    void timestampRegressionBeyondToleranceIsRejectedAsStale() {
        push("T1", 10_000, 100, 5, 10_000);

        // 时间戳倒退超过 maxLateMs（参考时间 10_000，tradeTime 1 000，滞后 9000ms）
        IngestResult stale = push("OLD", 1_000, 100, 5, 10_000);
        assertFalse(stale.isAccepted());
        assertEquals(IngestDecision.REJECTED_STALE, stale.getDecision());

        // 窗口内乱序但仍在容忍范围内：接受并标记为迟到回补
        IngestResult late = push("T2", 6_000, 120, 7, 10_500);
        assertTrue(late.isAccepted());
        assertEquals(IngestDecision.ACCEPTED_LATE_FILL, late.getDecision());

        Candle c6 = agg.getCandle("600000", 6000);
        assertEquals(120, c6.getClosePrice());
        assertEquals(7, c6.getVolume());
        log.info("[ASSERT] stale rejected; late fill window 6000 status={} volume={}",
                c6.getStatus(), c6.getVolume());
    }

    @Test
    void outOfOrderTicksWithinWindowRecomputeDeterministicOhlcv() {
        // 乱序到达同一窗口 [0,1000)
        push("T3", 300, 130, 3, 900);
        push("T1", 100, 100, 1, 900);
        push("T4", 400, 90, 4, 900);
        push("T2", 200, 110, 2, 900);

        Candle c = agg.getCandle("600000", 0);
        assertEquals(WindowStatus.FILLED, c.getStatus());
        // open/close 由 (tradeTime, tradeId) 排序决定，与到达顺序无关
        assertEquals(100, c.getOpenPrice());
        assertEquals(90, c.getClosePrice());
        assertEquals(130, c.getHighPrice());
        assertEquals(90, c.getLowPrice());
        assertEquals(10, c.getVolume());
        log.info("[ASSERT] out-of-order window 0 OHLCV={}/{}/{}/{}/{}",
                c.getOpenPrice(), c.getHighPrice(), c.getLowPrice(), c.getClosePrice(), c.getVolume());
    }

    @Test
    void windowBoundaryIsHalfOpenAndDeterministic() {
        // 窗口为 [floor(tradeTime/1000)*1000, +1000)，边界点归属右侧窗口
        IngestResult atBoundary = push("B1", 1000, 100, 1, 1000);
        assertEquals(1000L, atBoundary.getWindowStart());
        IngestResult justBefore = push("B0", 999, 99, 1, 1000);
        assertEquals(0L, justBefore.getWindowStart());

        // 负时间（历史数据）按 floorDiv 归属：-1ms 属于 [-1000,0)
        assertEquals(-1000L, agg.windowStartOf(-1));
        assertEquals(-1000L, agg.windowStartOf(-1000));
        assertEquals(-2000L, agg.windowStartOf(-1001));
        assertEquals(-1000L, agg.windowStartOf(-999));

        assertEquals(WindowStatus.FILLED, agg.windowStatus("600000", 0));
        assertEquals(WindowStatus.FILLED, agg.windowStatus("600000", 1000));
        log.info("[ASSERT] boundary: 999->window0, 1000->window1000, -1->window-1000");
    }

    @Test
    void invalidTicksRejectedWithDistinctReason() {
        Tick bad = new Tick();
        IngestResult r1 = ingest.ingest(bad, 100);
        assertEquals(IngestDecision.REJECTED_INVALID, r1.getDecision());

        bad.setSymbol("600000");
        bad.setTradeId("X");
        bad.setPrice(0);
        bad.setVolume(1);
        bad.setTradeTime(1);
        IngestResult r2 = ingest.ingest(bad, 100);
        assertEquals(IngestDecision.REJECTED_INVALID, r2.getDecision());
        assertTrue(r2.getReason().contains("price"));
        log.info("[ASSERT] invalid ticks rejected with distinct reasons: {} | {}",
                r1.getReason(), r2.getReason());
    }
}
