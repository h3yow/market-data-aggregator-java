package com.github.h3yow.marketdataaggregatorjava.tests.replay;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import com.github.h3yow.marketdataaggregatorjava.service.QueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件流独立重建视图测试。
 *
 * <p>客户端侧的“重放视图”规则（完全基于事件，不依赖区间快照查询）：
 * 按 seq 顺序应用事件，每个窗口保留其“最后一条事件携带的完整快照”，
 * 事件类型语义为 WINDOW_OPEN / WINDOW_UPDATE / GAP_MARKED / CORRECTION / SETTLED。
 * 要求任意已交付游标位置之后的重放结果与服务端区间 K 线查询逐窗口一致
 * （OHLCV、gap、corrected、version、settled 全部一致），且同一游标重放多次稳定。
 */
class EventReplayConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(EventReplayConsistencyTest.class);

    private static final String SYM = "RP2312";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;

    private MarketDataProperties props;
    private AggregationStore store;
    private IngestService ingestService;
    private QueryService queryService;
    private CursorService cursorService;

    @BeforeEach
    void setUp() {
        props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(60_000);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        ingestService = new IngestService(store);
        queryService = new QueryService(store, props);
        cursorService = new CursorService(store, props);
    }

    private void send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        ingestService.ingest(tick);
    }

    /**
     * 把一批事件按序折叠成“窗口 -> 该窗口最新快照”的重放视图（纯客户端逻辑）。
     */
    private static Map<Long, Candle> fold(List<CursorEvent> events) {
        Map<Long, Candle> view = new LinkedHashMap<>();
        for (CursorEvent e : events) {
            view.put(e.candle().windowStart(), e.candle());
        }
        return view;
    }

    /** 断言：重放视图与区间快照查询中同一批窗口逐字段一致。 */
    private void assertReplayEqualsSnapshot(Map<Long, Candle> replay, String tag) {
        Map<Long, Candle> snapshot = new LinkedHashMap<>();
        for (Candle c : queryService.queryCandles(SYM, null, null)) {
            snapshot.put(c.windowStart(), c);
        }
        assertEquals(snapshot.keySet(), replay.keySet(),
                tag + "：重放与快照的窗口集合必须一致");
        for (Long ws : snapshot.keySet()) {
            Candle a = replay.get(ws);
            Candle b = snapshot.get(ws);
            assertEquals(b, a, tag + "：窗口 " + (ws - T0) + " 重放快照与区间查询必须逐字段一致");
        }
    }

    /** 用游标分页把 afterSeq 之后的事件全部拉完，模拟真实客户端增量消费。 */
    private List<CursorEvent> pullAll(String cursor, int pageSize) {
        List<CursorEvent> all = new ArrayList<>();
        String cur = cursor;
        for (int guard = 0; guard < 10_000; guard++) {
            CursorPage page = cursorService.pull(SYM, cur, pageSize);
            all.addAll(page.events());
            cur = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        return all;
    }

    @Test
    @DisplayName("从头到尾重放事件（含开放窗口更新、缺口、回补、结算）：每个窗口与区间快照完全一致")
    void fullReplayMatchesSnapshot() {
        // 正常开窗 + 开放窗口内多笔更新（以前不产生事件，是重放/快照分歧的根源）
        send(1, 0, 10, 1);
        send(2, 200, 11, 2);
        send(3, 400, 9, 1);
        // 跨窗口制造缺口
        send(4, 3500, 20, 3);
        // 迟到回补缺口 + 迟到修正普通窗口
        send(5, 1200, 10.5, 7);
        send(6, 300, 10.2, 4);
        // 推进到结算部分窗口
        send(7, 90_000, 30, 5);

        List<CursorEvent> replayed = pullAll(null, 2); // 小页强制分页
        Map<Long, Candle> view = fold(replayed);
        log.info("[TEST-ASSERT] 重放 {} 条事件，窗口数={}，窗口={}",
                replayed.size(), view.size(),
                view.keySet().stream().map(ws -> (ws - T0) / W).toList());
        assertReplayEqualsSnapshot(view, "全量重放");

        // 抽查明细：开放窗口 w0 的 OHLCV 必须是最终值，且存在 WINDOW_UPDATE
        Candle w0 = view.get(T0);
        assertEquals(10.0, w0.open(), 1e-9);
        assertEquals(11.0, w0.high(), 1e-9);
        assertEquals(9.0, w0.low(), 1e-9);
        assertEquals(9.0, w0.close(), 1e-9);
        assertEquals(8L, w0.volume());
        assertTrue(w0.settled());
        assertTrue(replayed.stream().anyMatch(e -> e.type().name().equals("WINDOW_UPDATE")
                && e.candle().windowStart() == T0));
        // 被回补的缺口窗口也是正式 K 线
        Candle w1 = view.get(T0 + 1000);
        assertFalse(w1.gap());
        assertEquals(7L, w1.volume());
        assertTrue(w1.corrected());
    }

    @Test
    @DisplayName("从任意有效游标开始重放：仅对“游标之后出现最新状态”的窗口，与快照结果一致")
    void replayFromArbitraryCursorMatchesSnapshotForDeliveredWindows() {
        send(1, 0, 10, 1);
        send(2, 200, 11, 2);
        send(3, 3500, 20, 1);
        send(4, 4500, 21, 1);
        // 在 seq 的多个位置各取一个游标，验证“游标之后交付的最新事件”与快照一致
        long maxSeq = store.maxSeq(SYM);
        for (long after = 0; after <= maxSeq; after++) {
            String cursor = cursorService.issue(SYM, after);
            List<CursorEvent> tail = pullAll(cursor, 3);
            Map<Long, Candle> tailView = fold(tail);
            // 期望集合 = 快照中“最后一条事件 seq > after”的窗口；这里直接用事件窗口集比对快照
            Map<Long, Candle> snapshot = new LinkedHashMap<>();
            queryService.queryCandles(SYM, null, null).forEach(c -> {
                if (tailView.containsKey(c.windowStart())) {
                    snapshot.put(c.windowStart(), c);
                }
            });
            for (Long ws : tailView.keySet()) {
                assertEquals(snapshot.get(ws), tailView.get(ws),
                        "afterSeq=" + after + " 窗口 " + (ws - T0) / W + " 必须与快照一致");
            }
        }
        log.info("[TEST-ASSERT] 已在 {} 个游标位置逐一验证重放/快照一致", maxSeq + 1);
    }

    @Test
    @DisplayName("同一游标重放多次结果稳定（seq、类型、快照逐字节一致）")
    void sameCursorReplayIsStable() {
        send(1, 0, 10, 1);
        send(2, 1500, 12, 1);
        send(3, 1200, 11, 1);
        String cursor = cursorService.issue(SYM, 1L);

        List<CursorEvent> r1 = pullAll(cursor, 2);
        List<CursorEvent> r2 = pullAll(cursor, 5);
        List<CursorEvent> r3 = pullAll(cursor, 100);
        assertEquals(r1, r2);
        assertEquals(r2, r3);
        log.info("[TEST-ASSERT] 同一游标三种分页大小重放，事件数均={} 且完全相等", r1.size());
    }

    @Test
    @DisplayName("持续写入同时增量重放：多次追平后，重放视图始终与当时快照一致；最终全量也一致")
    void replayDuringConcurrentWritesStaysConsistent() throws Exception {
        // 预填一些历史
        send(1, 0, 10, 1);
        send(2, 200, 12, 1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger violations = new AtomicInteger();

        // 写线程：确定性脚本，跨越窗口并回补，制造 WINDOW_UPDATE/GAP/CORRECTION
        pool.submit(() -> {
            await(start);
            long id = 100;
            for (long offset = 1000; offset <= 20_000; offset += 250) {
                send(id++, offset, 10 + (offset % 700) / 100.0, 1);
                if (offset == 5000 || offset == 11_000) {
                    send(id++, (offset - 1000) + 100, 9.5, 2); // 迟到修正/回补
                }
            }
        });

        // 重放线程：反复从最新游标追平并把重放视图与快照逐窗口比对
        pool.submit(() -> {
            await(start);
            String cursor = null;
            Map<Long, Candle> view = new LinkedHashMap<>();
            for (int round = 0; round < 500; round++) {
                List<CursorEvent> batch = pullAll(cursor, 4);
                if (batch.isEmpty()) {
                    Thread.yield();
                    continue;
                }
                batch.forEach(e -> view.put(e.candle().windowStart(), e.candle()));
                cursor = cursorService.issue(SYM,
                        batch.get(batch.size() - 1).seq());
                // 仅校验“已通过事件看到最终版本”的窗口（开放窗口可能随后又更新）
                Map<Long, Candle> snapshotNow = new LinkedHashMap<>();
                queryService.queryCandles(SYM, null, null)
                        .forEach(c -> snapshotNow.put(c.windowStart(), c));
                for (Candle replayed : view.values()) {
                    Candle snap = snapshotNow.get(replayed.windowStart());
                    // 重放值不允许“倒退”或与当前快照矛盾：成交量单调不减，
                    // 且一旦快照结算/修正，重放侧最终也必须到达该版本
                    if (snap != null && replayed.volume() > snap.volume()) {
                        violations.incrementAndGet();
                    }
                }
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(0, violations.get(), "持续写入过程中重放视图不得出现成交量大于快照的倒退");

        // 写入结束后做一次全量重放，必须与最终快照逐窗口完全一致
        Map<Long, Candle> finalView = fold(pullAll(null, 7));
        assertReplayEqualsSnapshot(finalView, "并发写入后全量重放");
        log.info("[TEST-ASSERT] 并发写入结束，最终窗口数={} 全部与快照一致", finalView.size());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
