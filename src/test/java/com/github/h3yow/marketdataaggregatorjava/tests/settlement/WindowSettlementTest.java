package com.github.h3yow.marketdataaggregatorjava.tests.settlement;

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
 * 窗口结算规则测试。
 *
 * <p>结算条件（唯一确定、与迟到容忍规则一致）：窗口 w 同时满足
 * 已被更晚窗口推进（w &lt; maxWindowStart）且 watermark &gt;= w + W + tolerance。
 * 用例覆盖：结算边界恰好触发、SETTLED 事件顺序、结算后拒绝修正、
 * 结算前容忍内迟到仍可修正、缺口窗口以缺口形态结算、判定可复现。
 */
class WindowSettlementTest {

    private static final Logger log = LoggerFactory.getLogger(WindowSettlementTest.class);

    private static final String SYM = "S2312";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;
    private static final long TOL = 5000;

    private AggregationStore store;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(TOL);
        store = new AggregationStore(props);
        ingestService = new IngestService(store);
    }

    private IngestResult send(long id, long offset, double price) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, 1);
        log.info("[TEST-INPUT] {}", tick);
        IngestResult r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}", r.status(),
                r.windowStart() == null ? null : r.windowStart() - T0, r.reason());
        return r;
    }

    private Map<Long, Candle> candles() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    private List<CursorEvent> events() {
        return store.readEvents(SYM, 0, 10_000);
    }

    @Test
    @DisplayName("结算边界：watermark=w+W+tol-1 不结算；达到 w+W+tol 恰好结算且只结算该前缀")
    void settlesExactlyAtCutoff() {
        send(1, 0, 10);       // w0 开窗，watermark=0
        send(2, 1000, 11);    // w1 开窗；w0 截止线=6000，watermark=1000
        send(3, 5999, 12);    // 仍在 w5；watermark=5999 < 6000

        Candle w0Before = candles().get(T0);
        log.info("[TEST-ASSERT] watermark=5999 时 w0.settled={}", w0Before.settled());
        assertFalse(w0Before.settled(), "未到截止线 w+W+tol=6000，w0 不得提前结算");
        assertFalse(candles().get(T0 + 1000).settled());

        send(4, 6000, 13);    // 推进到 w6，watermark=6000 >= 6000
        Candle w0 = candles().get(T0);
        Candle w1 = candles().get(T0 + 1000);
        log.info("[TEST-ASSERT] watermark=6000 时 w0.settled={}, w1.settled={}",
                w0.settled(), w1.settled());
        assertTrue(w0.settled(), "达到截止线，w0 必须结算");
        assertFalse(w1.settled(), "w1 截止线=7000，尚不能结算");
        assertFalse(candles().get(T0 + 6000).settled(), "当前开放窗口永不结算");

        CursorEvent settled = events().stream()
                .filter(e -> e.type() == EventType.SETTLED
                        && e.candle().windowStart() == T0)
                .findFirst().orElseThrow();
        log.info("[TEST-ASSERT] SETTLED 事件 seq={}, candle.settled={}",
                settled.seq(), settled.candle().settled());
        assertTrue(settled.candle().settled());
        // SETTLED 必须出现在 w0 的开窗事件之后
        CursorEvent openW0 = events().stream()
                .filter(e -> e.candle().windowStart() == T0
                        && e.type() == EventType.WINDOW_OPEN)
                .findFirst().orElseThrow();
        assertTrue(settled.seq() > openW0.seq());
    }

    @Test
    @DisplayName("结算前：截止线未到，容忍内迟到成交仍可 CORRECTION；结算后同类迟到被判 STALE 且窗口不变")
    void correctionAllowedBeforeSettlementRejectedAfter() {
        send(1, 0, 10);
        send(2, 6000, 20);    // 推到 w6，watermark=6000：w0 恰好结算
        Candle w0Settled = candles().get(T0);
        assertTrue(w0Settled.settled());
        long versionAtSettle = w0Settled.version();
        double closeAtSettle = w0Settled.close();

        // w0 内任何成交 ts<=999，相对 watermark=6000 倒退 >=5001，严格超过容忍 -> STALE
        IngestResult late = send(3, 999, 99);
        log.info("[TEST-DECISION] 结算后迟到判定={}", late.status());
        assertEquals(IngestStatus.REJECTED_STALE, late.status());
        Candle w0After = candles().get(T0);
        assertEquals(versionAtSettle, w0After.version(), "结算窗口版本不得再变");
        assertEquals(closeAtSettle, w0After.close(), 1e-9, "结算窗口收盘价不得再变");
        // 结算之后不得再出现针对 w0 的 CORRECTION
        assertTrue(events().stream().noneMatch(e -> e.type() == EventType.CORRECTION
                && e.candle().windowStart() == T0));

        // 对照：w1 截止线=7000，尚未结算；ts=1000 相对水位倒退恰 5000（闭区间），应接受为 LATE
        IngestResult onBoundary = send(4, 1000, 21);
        log.info("[TEST-DECISION] 结算前边界迟到判定={}", onBoundary.status());
        assertEquals(IngestStatus.ACCEPTED_LATE, onBoundary.status());
        Candle w1 = candles().get(T0 + 1000);
        assertFalse(w1.settled());
        assertTrue(w1.corrected());
        // w1 收到修正后再推进水位越过其截止线：结算携带的必须是修正后的最终快照
        send(5, 7000, 30);
        Candle w1Settled = candles().get(T0 + 1000);
        assertTrue(w1Settled.settled());
        assertEquals(21.0, w1Settled.close(), 1e-9);
        CursorEvent w1SettledEvent = events().stream()
                .filter(e -> e.type() == EventType.SETTLED
                        && e.candle().windowStart() == T0 + 1000)
                .findFirst().orElseThrow();
        assertEquals(21.0, w1SettledEvent.candle().close(), 1e-9);
        assertTrue(w1SettledEvent.candle().corrected());
    }

    @Test
    @DisplayName("缺口窗口在截止线到达且未被回补时，以 gap=true 形态结算；结算后缺口不可被污染")
    void gapWindowSettlesAsGap() {
        send(1, 0, 10);
        send(2, 3500, 20);  // w0 -> w3，w1/w2 标缺口
        send(3, 9000, 30);  // watermark=9000：w2 截止线=3000+1000+5000=9000，w1/w2 结算
        Candle w2 = candles().get(T0 + 2000);
        log.info("[TEST-ASSERT] 缺口 w2 settled={}, gap={}", w2.settled(), w2.gap());
        assertTrue(w2.gap());
        assertTrue(w2.settled());
        assertNull(w2.open());

        CursorEvent settledGap = events().stream()
                .filter(e -> e.type() == EventType.SETTLED
                        && e.candle().windowStart() == T0 + 2000)
                .findFirst().orElseThrow();
        assertTrue(settledGap.candle().gap());
        assertTrue(settledGap.candle().settled());

        // 结算后的缺口：迟到回补必须被拒，缺口保持缺口
        IngestResult pollute = send(4, 2500, 88); // 倒退 6500 > 5000
        assertEquals(IngestStatus.REJECTED_STALE, pollute.status());
        assertTrue(candles().get(T0 + 2000).gap());
    }

    @Test
    @DisplayName("结算判定可复现：同一份确定性脚本跑两遍，SETTLED 的 seq/窗口顺序完全一致")
    void settlementDeterministicAcrossRuns() {
        Runnable script = () -> {
            send(1, 0, 10);
            send(2, 1000, 11);
            send(3, 7000, 12);
            send(4, 2500, 13); // w2 迟到修正（截止线未到）
            send(5, 9000, 14);
        };
        script.run();
        List<String> first = events().stream()
                .filter(e -> e.type() == EventType.SETTLED)
                .map(e -> e.seq() + ":" + (e.candle().windowStart() - T0))
                .toList();

        // 第二遍：全新 store，重放同一脚本
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(TOL);
        AggregationStore store2 = new AggregationStore(props);
        IngestService ingest2 = new IngestService(store2);
        for (long[] row : new long[][]{{1, 0, 10}, {2, 1000, 11}, {3, 7000, 12},
                {4, 2500, 13}, {5, 9000, 14}}) {
            ingest2.ingest(new TradeTick(SYM + "2", row[0], T0 + row[1], row[2], 1));
        }
        List<String> second = store2.readEvents(SYM + "2", 0, 10_000).stream()
                .filter(e -> e.type() == EventType.SETTLED)
                .map(e -> e.seq() + ":" + (e.candle().windowStart() - T0))
                .toList();
        log.info("[TEST-ASSERT] 第一次结算序列={}，第二次结算序列={}", first, second);
        assertEquals(first, second);
    }
}
