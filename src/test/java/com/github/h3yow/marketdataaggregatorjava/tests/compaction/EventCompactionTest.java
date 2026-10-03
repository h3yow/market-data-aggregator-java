package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.EventType;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件日志压缩测试。
 *
 * <p>压缩规则：已结算窗口只保留 WINDOW_SETTLED 事件（最终快照），seq 不重编号。
 * 因此压缩后：任意旧游标继续有效、重放视图与压缩前一致、事件不重不漏。
 */
class EventCompactionTest {

    private static final Logger log = LoggerFactory.getLogger(EventCompactionTest.class);

    private static final String SYM = "COMPACT";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;
    private static final long TOL = 5_000;

    private MarketDataProperties props;
    private AggregationStore store;
    private CursorService cursorService;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(TOL);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        cursorService = new CursorService(store, props);
        ingestService = new IngestService(store);
    }

    private void send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        var r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}", r.status(), r.windowStart(), r.reason());
    }

    /** 产生带丰富历史（OPEN/UPDATE/CORRECTION/SETTLED）的已结算窗口 + 未结算的最新窗口。 */
    private void ingestScript() {
        send(1, 100, 10.0, 1);      // w0 OPEN
        send(2, 200, 11.0, 2);      // w0 UPDATE
        send(3, 300, 9.0, 1);       // w0 UPDATE
        send(4, 3500, 12.0, 3);     // w3 OPEN，w1/w2 GAP
        send(5, 1200, 10.5, 7);     // w1 CORRECTION（回补缺口）
        send(6, 20_000, 15.0, 5);   // w20 OPEN，w4..w19 GAP；w0..w14 SETTLED
    }

    private Map<Long, Candle> replayFrom(String cursor) {
        Map<Long, Candle> view = new HashMap<>();
        String next = cursor;
        int guard = 0;
        while (guard++ < 10_000) {
            CursorPage page = cursorService.pull(SYM, next, 2);
            page.events().forEach(e -> view.put(e.candle().windowStart(), e.candle()));
            next = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        return view;
    }

    private Map<Long, Candle> snapshotView() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    @Test
    @DisplayName("压缩已结算窗口后：从头重放视图与压缩前及快照查询完全一致")
    void replayAfterCompactionMatchesPreCompactionAndSnapshot() {
        ingestScript();
        Map<Long, Candle> beforeView = replayFrom(null);
        Map<Long, Candle> snapshot = snapshotView();
        assertEquals(snapshot, beforeView, "前置条件：压缩前重放已与快照一致");
        long eventsBefore = store.readEvents(SYM, 0, 10_000).size();

        CompactionReport report = store.compact(SYM);
        log.info("[TEST-ASSERT] 压缩报告={}", report);
        assertTrue(report.settledWindows() > 0, "脚本应产生已结算窗口");
        assertTrue(report.droppedEvents() > 0, "应折叠掉已结算窗口的历史事件");
        assertEquals(eventsBefore - report.droppedEvents(), report.keptEvents());
        // 每个已结算窗口恰好保留一条 WINDOW_SETTLED
        long settledEvents = store.readEvents(SYM, 0, 10_000).stream()
                .filter(e -> e.type() == EventType.WINDOW_SETTLED).count();
        assertEquals(report.settledWindows(), settledEvents);

        Map<Long, Candle> afterView = replayFrom(null);
        log.info("[TEST-ASSERT] 压缩前视图窗口数={}, 压缩后={}", beforeView.size(), afterView.size());
        assertEquals(beforeView, afterView, "压缩后从头重放的视图必须与压缩前完全一致");
        assertEquals(snapshotView(), afterView, "压缩后重放视图必须与快照查询一致");
    }

    @Test
    @DisplayName("压缩后旧游标继续有效：从压缩前的游标增量拉取，不漏不重且收敛到快照")
    void oldCursorSurvivesCompaction() {
        ingestScript();

        // 客户端先消费前 2 条事件，手里留下旧游标和部分视图
        CursorPage firstPage = cursorService.pull(SYM, null, 2);
        String oldCursor = firstPage.nextCursor();
        Map<Long, Candle> clientView = new HashMap<>();
        Set<Long> seenSeqs = new HashSet<>();
        firstPage.events().forEach(e -> {
            clientView.put(e.candle().windowStart(), e.candle());
            seenSeqs.add(e.seq());
        });
        long cursorSeq = firstPage.events().get(firstPage.events().size() - 1).seq();

        CompactionReport report = store.compact(SYM);
        log.info("[TEST-ASSERT] 旧游标 seq={}, 压缩删除最大 seq={}", cursorSeq, report.maxDroppedSeq());
        assertTrue(report.maxDroppedSeq() > cursorSeq, "测试应覆盖“游标之后的事件被折叠”的场景");

        // 旧游标继续拉取：不抛异常、不重不漏，追平后视图 == 快照
        List<Long> newSeqs = new ArrayList<>();
        String next = oldCursor;
        int guard = 0;
        while (guard++ < 10_000) {
            CursorPage page = cursorService.pull(SYM, next, 2);
            for (CursorEvent e : page.events()) {
                assertTrue(e.seq() > cursorSeq, "只能收到游标之后的事件");
                assertTrue(seenSeqs.add(e.seq()), "同一 seq 不得重复下发: " + e.seq());
                newSeqs.add(e.seq());
                clientView.put(e.candle().windowStart(), e.candle());
            }
            next = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        log.info("[TEST-ASSERT] 压缩后旧游标追加收到 seqs={}", newSeqs);
        assertEquals(snapshotView(), clientView, "旧游标追平后的客户端视图必须与快照一致");
    }

    @Test
    @DisplayName("压缩幂等：重复压缩不再删除事件；压缩后继续写入 seq 不复用、重放仍一致")
    void compactionIdempotentAndSeqNeverReused() {
        ingestScript();
        store.compact(SYM);
        long maxSeqAfterCompact = store.maxSeq(SYM);

        CompactionReport second = store.compact(SYM);
        log.info("[TEST-ASSERT] 第二次压缩报告={}", second);
        assertEquals(0, second.droppedEvents(), "已压缩过的日志再次压缩不得删除任何事件");

        // 压缩后继续写入：新事件 seq 必须接着原有序号，绝不复用被删除的号段
        send(7, 21_000, 16.0, 1);   // w21，w15 结算
        send(8, 22_000, 17.0, 1);   // w22，w16 结算
        List<CursorEvent> all = store.readEvents(SYM, 0, 10_000);
        long maxSeq = all.stream().mapToLong(CursorEvent::seq).max().orElse(0);
        assertTrue(maxSeq > maxSeqAfterCompact, "压缩后新事件 seq 必须继续递增");
        assertEquals(maxSeq, store.maxSeq(SYM));
        Set<Long> seqs = new HashSet<>();
        all.forEach(e -> assertTrue(seqs.add(e.seq()), "seq 不得重复: " + e.seq()));

        assertEquals(snapshotView(), replayFrom(null), "压缩后继续写入，重放仍须与快照一致");
    }

    @Test
    @DisplayName("持续写入 + 并发压缩 + 持续拉取：客户端收集的事件不重，追平后视图与快照一致")
    void concurrentWriteCompactAndPullStaysConsistent() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean writerDone = new AtomicBoolean(false);

        // 写入线程：确定性脚本，100 个窗口、含窗口内多笔与迟到回补
        pool.submit(() -> {
            await(start);
            long id = 1;
            for (int w = 0; w < 100; w++) {
                ingestService.ingest(new TradeTick(SYM, id++, T0 + w * 1000L + 100, 10.0 + w, 1));
                ingestService.ingest(new TradeTick(SYM, id++, T0 + w * 1000L + 200, 11.0 + w, 2));
                if (w > 0 && w % 10 == 0) {
                    // 周期性回补前一个窗口（迟到但在容忍内）
                    ingestService.ingest(new TradeTick(SYM, id++, T0 + (w - 1) * 1000L + 300, 9.0, 5));
                }
            }
            writerDone.set(true);
        });

        // 压缩线程：写入期间反复压缩
        pool.submit(() -> {
            await(start);
            while (!writerDone.get()) {
                store.compact(SYM);
                Thread.sleep(1);
            }
            store.compact(SYM);
            return null;
        });

        // 拉取线程：客户端持续分页拉取直到追平
        List<CursorEvent> collected = new ArrayList<>();
        pool.submit(() -> {
            await(start);
            String cursor = null;
            while (true) {
                CursorPage page = cursorService.pull(SYM, cursor, 3);
                synchronized (collected) {
                    collected.addAll(page.events());
                }
                cursor = page.nextCursor();
                if (!page.hasMore() && writerDone.get() && page.events().isEmpty()) {
                    break;
                }
            }
            return null;
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        List<CursorEvent> events;
        synchronized (collected) {
            events = List.copyOf(collected);
        }
        Set<Long> seqs = new HashSet<>();
        long prev = 0;
        for (CursorEvent e : events) {
            assertTrue(e.seq() > prev, "客户端收到的事件 seq 必须严格递增: " + e.seq());
            assertTrue(seqs.add(e.seq()), "客户端不得收到重复 seq: " + e.seq());
            prev = e.seq();
        }
        Map<Long, Candle> view = new HashMap<>();
        events.forEach(e -> view.put(e.candle().windowStart(), e.candle()));
        Map<Long, Candle> snapshot = snapshotView();
        log.info("[TEST-ASSERT] 并发收集事件数={}, 重放窗口数={}, 快照窗口数={}",
                events.size(), view.size(), snapshot.size());
        assertEquals(snapshot, view, "持续写入+并发压缩下，客户端重放视图必须与快照一致");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
