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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 窗口结算（WINDOW_SETTLED）测试。
 *
 * <p>结算规则（唯一确定）：窗口 X 已结算 ⟺ 水位 {@code >= X.windowStart + W + tolerance}。
 * 此时窗口内任何时间戳的迟到数据都必然超过乱序容忍而被拒，窗口不再可能被修正。
 * 结算与现有的迟到/回补容忍规则严格一致。
 */
class WindowSettlementTest {

    private static final Logger log = LoggerFactory.getLogger(WindowSettlementTest.class);

    private static final String SYM = "SETTLE";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;
    private static final long TOL = 5_000;

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

    private IngestResult send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        IngestResult r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}", r.status(), r.windowStart(), r.reason());
        return r;
    }

    private Map<Long, Candle> candles() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    private List<CursorEvent> eventsOf(EventType type) {
        return store.readEvents(SYM, 0, 10_000).stream()
                .filter(e -> e.type() == type)
                .toList();
    }

    @Test
    @DisplayName("结算边界唯一确定：水位 < 窗口终点+容忍 时不结算，恰好达到时结算且只结算一次")
    void settlementBoundaryIsExactAndEmittedOnce() {
        send(1, 100, 10.0, 1);          // w0 开窗，水位 T0+100
        // w0 的结算点 = w0 终点(T0+1000) + 容忍(5000) = 水位 >= T0+6000
        send(2, 5_999, 20.0, 1);        // 水位 T0+5999，差 1ms，w0 不得结算
        assertTrue(eventsOf(EventType.WINDOW_SETTLED).isEmpty(), "水位未达结算点，不允许结算");
        assertFalse(candles().get(T0).settled(), "快照中 w0 应未结算");

        send(3, 6_000, 21.0, 1);        // 水位 T0+6000，恰好到达结算点
        List<CursorEvent> settled = eventsOf(EventType.WINDOW_SETTLED);
        log.info("[TEST-ASSERT] 结算事件={}", settled);
        assertEquals(1, settled.size(), "恰好到达结算点必须结算 w0");
        assertEquals(T0, settled.get(0).candle().windowStart());
        assertTrue(candles().get(T0).settled(), "快照中 w0 应已结算");
        // 结算事件携带的就是最终快照，与快照查询一致
        assertEquals(candles().get(T0), settled.get(0).candle());

        send(4, 7_000, 22.0, 1);        // 水位继续推进：w1(缺口) 到达结算点，但 w0 不得重复结算
        List<CursorEvent> allSettled = eventsOf(EventType.WINDOW_SETTLED);
        long w0SettledCount = allSettled.stream()
                .filter(e -> e.candle().windowStart() == T0).count();
        log.info("[TEST-ASSERT] 全部结算事件={}",
                allSettled.stream().map(e -> e.candle().windowStart()).toList());
        assertEquals(1, w0SettledCount, "同一窗口只能结算一次");
    }

    @Test
    @DisplayName("结算前迟到数据可修正（CORRECTION），结算后同样的迟到数据被拒（REJECTED_STALE）且窗口不变")
    void correctionAllowedBeforeSettlementRejectedAfter() {
        send(1, 100, 10.0, 1);          // w0
        send(2, 2_000, 20.0, 1);        // w2，水位 T0+2000，w0 未结算
        IngestResult late = send(3, 500, 99.0, 9); // 迟到 1500ms <= 容忍，修正 w0
        assertEquals(IngestStatus.ACCEPTED_LATE, late.status());
        Candle corrected = candles().get(T0);
        assertTrue(corrected.corrected());
        assertEquals(99.0, corrected.close(), 1e-9);
        assertFalse(corrected.settled());

        send(4, 6_000, 30.0, 1);        // 水位 T0+6000，w0 结算
        assertTrue(candles().get(T0).settled());
        IngestResult stale = send(5, 900, 1.0, 1); // 结算后任何 w0 内迟到数据必超容忍
        assertEquals(IngestStatus.REJECTED_STALE, stale.status());
        Candle after = candles().get(T0);
        log.info("[TEST-ASSERT] 结算后窗口={}", after);
        assertEquals(corrected.close(), after.close(), "结算后窗口内容不得再变");
        assertEquals(corrected.volume(), after.volume());
        assertEquals(corrected.version(), after.version());
    }

    @Test
    @DisplayName("缺口窗口同样会结算：结算后缺口为最终状态，迟到回补被拒")
    void gapWindowSettlesAndBecomesFinal() {
        send(1, 100, 10.0, 1);          // w0
        send(2, 3_000, 20.0, 1);        // w3，w1/w2 缺口
        send(3, 9_000, 30.0, 1);        // 水位 T0+9000：w0/w1/w2 全部结算
        Map<Long, Candle> m = candles();
        assertTrue(m.get(T0 + 1000).gap() && m.get(T0 + 1000).settled(), "w1 应为已结算缺口");
        assertTrue(m.get(T0 + 2000).gap() && m.get(T0 + 2000).settled(), "w2 应为已结算缺口");
        // 结算事件里缺口窗口也各有一条，携带最终缺口快照
        // （水位 T0+9000 时结算点为 windowStart <= T0+3000，w3 也恰好结算）
        List<CursorEvent> settled = eventsOf(EventType.WINDOW_SETTLED);
        assertEquals(4, settled.size());
        assertEquals(List.of(T0, T0 + 1000, T0 + 2000, T0 + 3000),
                settled.stream().map(e -> e.candle().windowStart()).toList());

        IngestResult backfill = send(4, 1_200, 10.5, 7); // 试图回补已结算缺口
        assertEquals(IngestStatus.REJECTED_STALE, backfill.status());
        assertTrue(candles().get(T0 + 1000).gap(), "已结算缺口不得被回补污染");
    }

    @Test
    @DisplayName("窗口生命周期事件可区分：WINDOW_OPEN -> WINDOW_UPDATE -> CORRECTION -> WINDOW_SETTLED")
    void windowLifecycleEventsAreDistinguishable() {
        send(1, 100, 10.0, 1);          // w0 WINDOW_OPEN
        send(2, 200, 11.0, 1);          // w0 WINDOW_UPDATE
        send(3, 2_000, 20.0, 1);        // w2 开窗（w1 缺口）
        send(4, 500, 12.0, 1);          // 迟到修正 w0 -> CORRECTION
        send(5, 6_000, 30.0, 1);        // 水位 T0+6000，w0 结算 -> WINDOW_SETTLED

        List<CursorEvent> w0Events = store.readEvents(SYM, 0, 10_000).stream()
                .filter(e -> e.candle().windowStart() == T0)
                .toList();
        log.info("[TEST-ASSERT] w0 生命周期={}",
                w0Events.stream().map(e -> e.seq() + ":" + e.type()).toList());
        assertEquals(List.of(EventType.WINDOW_OPEN, EventType.WINDOW_UPDATE,
                        EventType.CORRECTION, EventType.WINDOW_SETTLED),
                w0Events.stream().map(CursorEvent::type).toList());
        // 客户端可据此判断：只有 WINDOW_SETTLED 之后的窗口才可安全落库
        Candle finalCandle = w0Events.get(w0Events.size() - 1).candle();
        assertTrue(finalCandle.settled());
        assertEquals(candles().get(T0), finalCandle);
    }

    @Test
    @DisplayName("结算判定可复现：同一脚本跑两遍，事件序列（类型+快照）完全一致")
    void settlementIsDeterministicAndReproducible() {
        java.util.function.Supplier<List<String>> runScript = () -> {
            MarketDataProperties props = new MarketDataProperties();
            props.setWindowSizeMillis(W);
            props.setOutOfOrderToleranceMillis(TOL);
            AggregationStore s = new AggregationStore(props);
            IngestService ing = new IngestService(s);
            long[][] script = {
                    {1, 100, 1000, 1}, {2, 200, 1100, 2}, {3, 3500, 1200, 3},
                    {4, 1200, 1050, 7}, {5, 6_000, 1300, 1}, {6, 20_000, 1500, 5},
            };
            for (long[] row : script) {
                ing.ingest(new TradeTick(SYM, row[0], T0 + row[1], row[2] / 100.0d, row[3]));
            }
            return s.readEvents(SYM, 0, 10_000).stream()
                    .map(e -> e.seq() + "|" + e.type() + "|" + e.candle())
                    .toList();
        };
        List<String> first = runScript.get();
        List<String> second = runScript.get();
        log.info("[TEST-ASSERT] 第一遍事件数={}, 第二遍事件数={}", first.size(), second.size());
        assertEquals(first, second, "同一输入脚本的事件流必须逐条一致（含结算事件）");
        assertTrue(first.stream().anyMatch(e -> e.contains("WINDOW_SETTLED")), "脚本应触发结算");
    }
}
