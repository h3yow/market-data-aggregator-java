package com.github.h3yow.marketdataaggregatorjava.tests;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.model.Candle;
import com.github.h3yow.marketdataaggregatorjava.model.CandleChange;
import com.github.h3yow.marketdataaggregatorjava.model.IngestDecision;
import com.github.h3yow.marketdataaggregatorjava.model.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.model.PullResponse;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import com.github.h3yow.marketdataaggregatorjava.model.WindowStatus;
import com.github.h3yow.marketdataaggregatorjava.service.CandleAggregationService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorCodec;
import com.github.h3yow.marketdataaggregatorjava.service.InvalidCursorException;
import com.github.h3yow.marketdataaggregatorjava.service.TickIngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖：无行情缺口识别、迟到回补修正且不重复累计、游标增量不重不漏与非法游标拒绝。
 */
class GapAndCursorTest {

    private static final Logger log = LoggerFactory.getLogger(GapAndCursorTest.class);

    private static final String SYM = "IF2409";

    private AggregatorProperties props;
    private TickIngestService ingest;
    private CandleAggregationService agg;
    private CursorCodec codec;

    @BeforeEach
    void setUp() {
        props = new AggregatorProperties();
        props.setWindowMs(1000);
        props.setMaxLateMs(60_000);
        props.setPullBatchSize(2);
        codec = new CursorCodec(props);
        agg = new CandleAggregationService(props, codec);
        ingest = new TickIngestService(agg, props);
    }

    private IngestResult push(String id, long t, long price, long vol, long now) {
        Tick tick = new Tick();
        tick.setSymbol(SYM);
        tick.setTradeId(id);
        tick.setPrice(price);
        tick.setVolume(vol);
        tick.setTradeTime(t);
        IngestResult r = ingest.ingest(tick, now);
        log.info("[CASE] input tick=(symbol={},tradeId={},price={},volume={},tradeTime={},now={}) "
                        + "=> accepted={}, decision={}, windowStart={}, reason={}",
                SYM, id, price, vol, t, now, r.isAccepted(), r.getDecision(),
                r.getWindowStart(), r.getReason());
        return r;
    }

    @Test
    void gapsAreMarkedWhenFrontierAdvances() {
        push("A", 100, 100, 10, 100);
        // 下一笔直接跳到窗口 3000：窗口 [1000,2000) 与 [2000,3000) 必须被物化为 GAP
        push("B", 3050, 200, 20, 3050);

        assertEquals(WindowStatus.FILLED, agg.windowStatus(SYM, 0));
        assertEquals(WindowStatus.GAP, agg.windowStatus(SYM, 1000));
        assertEquals(WindowStatus.GAP, agg.windowStatus(SYM, 2000));
        assertEquals(WindowStatus.FILLED, agg.windowStatus(SYM, 3000));

        Candle gap = agg.getCandle(SYM, 1000);
        assertEquals(0, gap.getVolume());
        log.info("[ASSERT] windows status: 0=FILLED, 1000=GAP(volume={}), 2000=GAP, 3000=FILLED",
                gap.getVolume());
    }

    @Test
    void lateDataBackfillsGapWithoutDoubleCounting() {
        push("A", 100, 100, 10, 100);
        push("B", 3050, 200, 20, 3050);

        // 迟到数据回补窗口 [2000,3000)
        IngestResult back = push("L1", 2100, 150, 7, 3500);
        assertTrue(back.isAccepted());
        assertEquals(IngestDecision.ACCEPTED_LATE_FILL, back.getDecision());

        Candle c2 = agg.getCandle(SYM, 2000);
        assertEquals(WindowStatus.BACKFILLED, c2.getStatus());
        assertEquals(150, c2.getOpenPrice());
        assertEquals(150, c2.getClosePrice());
        assertEquals(7, c2.getVolume());

        // 同 tradeId 再推一次必须被幂等拒绝，回补窗口的量不得重复累计
        IngestResult dup = push("L1", 2100, 150, 7, 3600);
        assertFalse(dup.isAccepted());
        assertEquals(IngestDecision.REJECTED_DUPLICATE, dup.getDecision());
        assertEquals(7, agg.getCandle(SYM, 2000).getVolume());

        // 再迟到一笔同窗口，确定性重算 OHLCV
        push("L2", 2200, 180, 3, 3700);
        c2 = agg.getCandle(SYM, 2000);
        assertEquals(WindowStatus.BACKFILLED, c2.getStatus());
        assertEquals(150, c2.getOpenPrice());
        assertEquals(180, c2.getClosePrice());
        assertEquals(180, c2.getHighPrice());
        assertEquals(150, c2.getLowPrice());
        assertEquals(10, c2.getVolume());
        log.info("[ASSERT] backfilled window 2000 OHLCV={}/{}/{}/{}/{} status={}",
                c2.getOpenPrice(), c2.getHighPrice(), c2.getLowPrice(), c2.getClosePrice(),
                c2.getVolume(), c2.getStatus());

        // 其它窗口不受影响
        assertEquals(10, agg.getCandle(SYM, 0).getVolume());
        assertEquals(20, agg.getCandle(SYM, 3000).getVolume());
        assertEquals(WindowStatus.GAP, agg.windowStatus(SYM, 1000));
    }

    @Test
    void incrementalPullIsExactlyOnceAcrossPagesAndRepeats() {
        push("A", 100, 100, 10, 100);
        push("B", 3050, 200, 20, 3050);
        push("L", 2100, 150, 7, 3500);
        // 变更日志顺序：W0 filled, W1 gap, W2 gap, W3 filled, W2 backfilled => 5 条，seq 1..5

        List<CandleChange> collected = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < 10; page++) {
            PullResponse resp = agg.pull(SYM, cursor, 5_000);
            collected.addAll(resp.getChanges());
            cursor = resp.getNextCursor();
            if (!resp.isHasMore()) {
                break;
            }
        }
        assertEquals(5, collected.size());
        for (int i = 0; i < collected.size(); i++) {
            assertEquals(i + 1, collected.get(i).getSeq());
        }
        log.info("[ASSERT] drained {} changes across pages, seq strictly increasing", collected.size());

        // 用同一游标重复拉取：不重不漏（空集），游标保持稳定
        PullResponse repeat = agg.pull(SYM, cursor, 5_000);
        assertTrue(repeat.getChanges().isEmpty());
        assertFalse(repeat.isHasMore());
        assertEquals(cursor, repeat.getNextCursor());

        // 回补再产生一条新变更，续拉只能拿到增量
        push("L2", 2200, 180, 3, 3800);
        PullResponse more = agg.pull(SYM, cursor, 5_000);
        assertEquals(1, more.getChanges().size());
        assertEquals(6, more.getChanges().get(0).getSeq());
        assertEquals(WindowStatus.BACKFILLED, more.getChanges().get(0).getStatus());
        log.info("[ASSERT] resume pull returned only seq=6: {}", more.getChanges().get(0));
    }

    @Test
    void invalidAndExpiredCursorsAreRejectedWithDistinctMessages() {
        push("A", 100, 100, 10, 100);

        InvalidCursorException garbage = assertThrows(InvalidCursorException.class,
                () -> agg.pull(SYM, "garbage-cursor", 5_000));
        log.info("[ASSERT] garbage cursor rejected: {}", garbage.getMessage());

        String valid = codec.encode(SYM, 1L, 1_000);
        InvalidCursorException expired = assertThrows(InvalidCursorException.class,
                () -> agg.pull(SYM, valid, 1_000 + props.getCursorTtlMs() + 1));
        assertTrue(expired.getMessage().contains("expired"));
        log.info("[ASSERT] expired cursor rejected: {}", expired.getMessage());

        // 跨标的使用游标
        InvalidCursorException cross = assertThrows(InvalidCursorException.class,
                () -> agg.pull("OTHER", valid, 1_500));
        assertTrue(cross.getMessage().contains("different symbol"));
        log.info("[ASSERT] cross-symbol cursor rejected: {}", cross.getMessage());
    }
}
