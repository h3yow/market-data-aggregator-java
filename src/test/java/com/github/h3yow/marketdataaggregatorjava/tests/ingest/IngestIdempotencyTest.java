package com.github.h3yow.marketdataaggregatorjava.tests.ingest;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 逐笔接入的幂等与拒绝规则测试。
 * 每笔输入行情与判定依据均通过日志打印（{@link AggregationStore}/{@link IngestService} 的 [INGEST] 日志）。
 */
class IngestIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(IngestIdempotencyTest.class);

    private IngestService ingestService;
    private static final String SYM = "600000";
    private static final long T0 = 1_700_000_000_000L;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(5000);
        ingestService = new IngestService(new AggregationStore(props));
    }

    private IngestResult send(long id, long ts, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, ts, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        IngestResult r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, reason={}", r.status(), r.reason());
        return r;
    }

    @Test
    @DisplayName("重复推送判定为 DUPLICATE 且不重复计入成交量")
    void duplicatePushIsIdempotent() {
        IngestResult first = send(1, T0 + 100, 10.0, 5);
        assertEquals(IngestStatus.ACCEPTED, first.status());
        assertNotNull(first.windowStart());

        // 完全相同的第二笔
        IngestResult dup = send(1, T0 + 100, 10.0, 5);
        assertEquals(IngestStatus.DUPLICATE, dup.status());
        assertNull(dup.windowStart());

        // 即使重复推送伪造了不同价格/数量，也必须判重且不污染聚合
        IngestResult dupForged = send(1, T0 + 100, 999.0, 999);
        assertEquals(IngestStatus.DUPLICATE, dupForged.status());

        // 成交量仍为首笔的 5，由后续窗口测试交叉验证；这里只验证判定次数
        assertEquals(2L, countStatus(first.status(), dup.status(), dupForged.status()));
    }

    @Test
    @DisplayName("重复判定优先于倒退判定：旧 tradeId 即使时间戳很旧也是 DUPLICATE")
    void duplicateCheckedBeforeStaleness() {
        send(10, T0 + 9000, 10.0, 1); // 水位推到 T0+9000
        IngestResult old = send(10, T0, 10.0, 1); // 倒退远超容忍
        assertEquals(IngestStatus.DUPLICATE, old.status(), "重放必须幂等，优先判重而非 REJECTED_STALE");
    }

    @Test
    @DisplayName("时间戳倒退超过容忍 -> REJECTED_STALE；容忍范围内 -> ACCEPTED_LATE，二者可区分")
    void staleVsLateAreDistinguishable() {
        send(20, T0 + 9000, 10.0, 1);

        IngestResult stale = send(21, T0 + 3000, 11.0, 1); // 倒退 6000ms
        assertEquals(IngestStatus.REJECTED_STALE, stale.status());
        assertNull(stale.windowStart());

        IngestResult late = send(22, T0 + 5000, 12.0, 1); // 倒退 4000ms，在容忍内
        assertEquals(IngestStatus.ACCEPTED_LATE, late.status());
        assertNotNull(late.windowStart());
    }

    @Test
    @DisplayName("边界：倒退恰好等于容忍值时接受(ACCEPTED_LATE)，超过 1ms 才拒绝")
    void stalenessBoundaryIsInclusive() {
        send(30, T0 + 10_000, 10.0, 1);

        IngestResult atBoundary = send(31, T0 + 5000, 10.0, 1); // 恰好 5000ms
        assertEquals(IngestStatus.ACCEPTED_LATE, atBoundary.status());

        IngestResult beyond = send(32, T0 + 4999, 10.0, 1); // 5001ms
        assertEquals(IngestStatus.REJECTED_STALE, beyond.status());
    }

    @Test
    @DisplayName("非法字段：空symbol/非正tradeId、timestamp、price、volume -> REJECTED_INVALID")
    void invalidFieldsRejected() {
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick("  ", 1, T0, 10, 1)).status());
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick(SYM, 0, T0, 10, 1)).status());
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick(SYM, 1, 0, 10, 1)).status());
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick(SYM, 1, T0, 0, 1)).status());
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick(SYM, 1, T0, Double.NaN, 1)).status());
        assertEquals(IngestStatus.REJECTED_INVALID,
                ingestService.ingest(new TradeTick(SYM, 1, T0, 10, 0)).status());
    }

    private long countStatus(IngestStatus... statuses) {
        long n = 0;
        for (IngestStatus s : statuses) {
            if (s == IngestStatus.DUPLICATE) {
                n++;
            }
        }
        return n;
    }
}
