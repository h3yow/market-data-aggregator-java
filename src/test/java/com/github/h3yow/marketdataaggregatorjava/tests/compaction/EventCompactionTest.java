package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CursorException;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件日志压缩测试。
 *
 * <p>压缩规则：已结算窗口只保留 WINDOW_SETTLED 事件（最终快照），seq 不重编号，
 * 并记录“压缩前沿” {@code compactedThroughSeq}（被删事件 seq 的高水位）。
 *
 * <p>本类覆盖用户关心的全部路径：
 * <ol>
 *   <li>{@link #sameCursorAfterCompaction_byteIdenticalWhenPositionSurvives()}：
 *       游标位置不在删除范围内时，压缩前后同一游标看到的事件逐条一致（不重不漏）；</li>
 *   <li>{@link #sameCursorAfterCompaction_rejectedWithRecoveryWhenPositionCompacted()}：
 *       游标续拉位置被压缩掉时，明确以 COMPACTED 拒绝、说明原因并返回
 *       resumeSeq=0 + 新游标；用新游标重放收敛到区间查询；</li>
 *   <li>{@link #repeatedCompactionIsIdempotentAndFrontierMonotonic()}：
 *       反复压缩幂等、前沿单调不减、压缩后新事件 seq 不复用、旧游标判定不反复；</li>
 *   <li>{@link #concurrentWriteCompactPull_staysConsistentWithSnapshot()}：
 *       持续写入 + 反复压缩 + 多客户端持续拉取，按恢复协议重置后，每个客户端
 *       收到的事件不重复、seq 只向前，重建视图与区间查询完全一致。</li>
 * </ol>
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

    /**
     * 确定性脚本：w0 含 OPEN/2 条 UPDATE；w3 OPEN 同时标 w1/w2 缺口；
     * 迟到回补 w1（CORRECTION）；水位推进到 w20，结算 w0..w14。
     * 事件布局（经探针确认）：seq 1..39；压缩删除已结算窗口的非结算事件 1..18，
     * 保留 19..39（w15..w19 缺口的 GAP_MARKED + w20 OPEN + 15 条 SETTLED），
     * 压缩前沿 frontier=18。
     */
    private void ingestScript() {
        send(1, 100, 10.0, 1);      // seq1  w0 OPEN
        send(2, 200, 11.0, 2);      // seq2  w0 UPDATE
        send(3, 300, 9.0, 1);       // seq3  w0 UPDATE
        send(4, 3500, 12.0, 3);     // seq4  w3 OPEN，seq5/6 w1/w2 GAP
        send(5, 1200, 10.5, 7);     // seq7  w1 CORRECTION（回补缺口）
        send(6, 20_000, 15.0, 5);   // seq24 w20 OPEN，w4..w19 GAP；w0..w14 SETTLED
    }

    private Map<Long, Candle> snapshotView() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    /** 用指定游标把事件流一路拉到底，按到达顺序收集事件（不经任何去重）。 */
    private List<CursorEvent> drain(String cursor, int pageSize) {
        List<CursorEvent> out = new ArrayList<>();
        String next = cursor;
        int guard = 0;
        while (guard++ < 10_000) {
            CursorPage page = cursorService.pull(SYM, next, pageSize);
            out.addAll(page.events());
            next = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        return out;
    }

    private Map<Long, Candle> viewOf(List<CursorEvent> events) {
        Map<Long, Candle> view = new HashMap<>();
        events.forEach(e -> view.put(e.candle().windowStart(), e.candle()));
        return view;
    }

    // ---------------------------------------------------------------------
    // 路径一：游标位置未被压缩 -> 压缩前后逐条一致
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("同游标(位置存活)压缩前后拉取的事件序列逐条一致，不漏不重")
    void sameCursorAfterCompaction_byteIdenticalWhenPositionSurvives() {
        ingestScript();
        Map<Long, Candle> snapshot = snapshotView();
        assertEquals(snapshot, viewOf(drain(null, 5)), "前置：压缩前重放视图已与区间查询一致");

        // 判定依据：脚本压缩前沿为 18；只要游标 seq >= 18，其“之后”的事件一条都不会被删
        CompactionReport report = store.compact(SYM);
        log.info("[TEST-ASSERT] 压缩报告 settledWindows={}, dropped={}, kept={}, maxDroppedSeq={}, frontier={}",
                report.settledWindows(), report.droppedEvents(), report.keptEvents(),
                report.maxDroppedSeq(), report.compactedThroughSeq());
        assertEquals(18L, report.compactedThroughSeq(), "脚本的压缩前沿应为 18");

        // 两个有代表性的存活位置：seq=18（恰好等于前沿）与 seq=21（前沿之后、缺口事件）
        for (long position : List.of(18L, 21L)) {
            String cursor = cursorService.issue(SYM, position);

            List<CursorEvent> before = drainAtPositionBeforeCompaction(position);
            List<CursorEvent> after = drain(cursor, 4);

            List<Long> beforeSeqs = before.stream().map(CursorEvent::seq).toList();
            List<Long> afterSeqs = after.stream().map(CursorEvent::seq).toList();
            log.info("[TEST-ASSERT] 游标 seq={} 压缩前序列={} 压缩后序列={}", position, beforeSeqs, afterSeqs);

            assertEquals(beforeSeqs, afterSeqs,
                    "位置存活的旧游标压缩前后必须返回完全相同的 seq 序列");
            assertFalse(after.isEmpty(), "seq=18 之后仍有保留事件");
            for (CursorEvent e : after) {
                assertTrue(e.seq() > position, "只能返回游标之后的事件: " + e.seq());
            }
            assertEquals(before.size(), new HashSet<>(afterSeqs).size(), "不重不漏");
            assertEquals(before, after, "事件内容（类型+快照）也必须逐条一致");
            assertEquals(39L, after.get(after.size() - 1).seq(), "应一直拉到压缩后日志末尾");
        }
    }

    /** 独立地构造“未压缩世界”，给出 seq 严格大于 position 的期望事件序列。 */
    private List<CursorEvent> drainAtPositionBeforeCompaction(long position) {
        MarketDataProperties p2 = new MarketDataProperties();
        p2.setWindowSizeMillis(W);
        p2.setOutOfOrderToleranceMillis(TOL);
        p2.setCursorTtlMillis(3_600_000);
        AggregationStore pristine = new AggregationStore(p2);
        IngestService ingest2 = new IngestService(pristine);
        long[][] ticks = {{1, 100, 1}, {2, 200, 2}, {3, 300, 1}, {4, 3500, 3}, {5, 1200, 7}, {6, 20_000, 5}};
        double[] prices = {10.0, 11.0, 9.0, 12.0, 10.5, 15.0};
        for (int i = 0; i < ticks.length; i++) {
            ingest2.ingest(new TradeTick(SYM, (long) ticks[i][0], T0 + (long) ticks[i][1],
                    prices[i], (long) ticks[i][2]));
        }
        return pristine.readEvents(SYM, position, 10_000);
    }

    // ---------------------------------------------------------------------
    // 路径二：游标续拉位置被压缩 -> 明确拒绝 + 返回恢复起点，重放收敛
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("同游标(位置被压缩)被明确拒绝(COMPACTED)，携带resumeSeq=0与新游标，重放后与区间查询一致")
    void sameCursorAfterCompaction_rejectedWithRecoveryWhenPositionCompacted() {
        ingestScript();

        // 客户端在压缩前消费到 seq=3（其后续拉位置落在被删区间 1..18 内）
        CursorPage firstPage = cursorService.pull(SYM, null, 3);
        String staleCursor = firstPage.nextCursor();
        List<Long> consumed = firstPage.events().stream().map(CursorEvent::seq).toList();
        log.info("[TEST-INPUT] 压缩前客户端已消费 seqs={}，持有旧游标继续拉取", consumed);

        // 未压缩时用该游标本应看到 seq=4..39
        List<Long> expectedIfNotCompacted = drainAtPositionBeforeCompaction(3).stream()
                .map(CursorEvent::seq).toList();
        log.info("[TEST-ASSERT] 判定依据：未压缩时该游标应看到 {}", expectedIfNotCompacted);

        CompactionReport report = store.compact(SYM);
        log.info("[TEST-ASSERT] 压缩后前沿={}（>3），续拉位置内的事件已被删除",
                report.compactedThroughSeq());
        assertTrue(report.compactedThroughSeq() > 3, "用例必须覆盖位置被压缩的场景");

        // 必须与正常拉取区分：明确拒绝，而不是静默返回对不上的半截序列
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, staleCursor, 100));
        log.info("[TEST-ASSERT] 拒绝原因={}，说明={}", ex.reason(), ex.getMessage());
        assertEquals(CursorException.Reason.COMPACTED, ex.reason(),
                "续拉位置被压缩时必须以 COMPACTED 明确拒绝，不能静默对不上");
        // 恢复起点 = 压缩后日志首条保留事件（seq=19）之前 = 18
        assertEquals(18L, ex.resumeSeq(), "可恢复起点必须指向压缩后日志开头(19)之前");
        assertNotNull(ex.resumeCursor(), "必须返回新游标");

        // 空游标 / 其它落在删除区间内的位置同样被拒绝
        for (long pos : List.of(0L, 1L, 17L)) {
            CursorException e2 = assertThrows(CursorException.class,
                    () -> cursorService.pull(SYM, cursorService.issue(SYM, pos), 100));
            assertEquals(CursorException.Reason.COMPACTED, e2.reason(),
                    "seq=" + pos + " 位于删除区间内，必须拒绝");
            assertEquals(18L, e2.resumeSeq(),
                    "seq=" + pos + " 的恢复起点同样指向压缩后日志开头(18)");
            assertNotNull(e2.resumeCursor());
        }

        // 拿到恢复起点后继续往下拉：重建视图与区间查询逐条窗口一致，且事件不重复
        List<CursorEvent> resumed = drain(ex.resumeCursor(), 7);
        List<Long> resumedSeqs = resumed.stream().map(CursorEvent::seq).toList();
        log.info("[TEST-ASSERT] 用新游标重放收到 seqs={}", resumedSeqs);
        assertEquals(resumedSeqs.size(), new HashSet<>(resumedSeqs).size(), "恢复重放不得重复");
        assertTrue(resumedSeqs.stream().allMatch(s -> s > 0));
        assertEquals(snapshotView(), viewOf(resumed),
                "按恢复起点重放重建的每个窗口状态必须与区间查询一致");
        // 已结算窗口最终状态一个不少：恢复流里包含全部 15 个已结算窗口的 SETTLED
        long settledInResume = resumed.stream()
                .filter(e -> e.candle().settled()).map(e -> e.candle().windowStart()).distinct().count();
        assertEquals(15L, settledInResume, "已结算窗口的最终状态不得因压缩丢失");
    }

    // ---------------------------------------------------------------------
    // 反复压缩：幂等、前沿单调、seq 不复用
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("反复压缩幂等且前沿单调不减；压缩后写入的新事件seq不复用；旧游标判定不反复")
    void repeatedCompactionIsIdempotentAndFrontierMonotonic() {
        ingestScript();
        CompactionReport first = store.compact(SYM);
        long frontier = first.compactedThroughSeq();
        log.info("[TEST-ASSERT] 首次压缩 dropped={}, frontier={}", first.droppedEvents(), frontier);
        assertTrue(first.droppedEvents() > 0);

        CompactionReport second = store.compact(SYM);
        CompactionReport third = store.compact(SYM);
        log.info("[TEST-ASSERT] 二次/三次压缩 dropped={}/{}, frontier={}/{}",
                second.droppedEvents(), third.droppedEvents(),
                second.compactedThroughSeq(), third.compactedThroughSeq());
        assertEquals(0, second.droppedEvents(), "重复压缩不能再删除事件");
        assertEquals(0, third.droppedEvents(), "第三次压缩同样幂等");
        assertEquals(frontier, second.compactedThroughSeq(), "压缩前沿不得回退");
        assertEquals(frontier, third.compactedThroughSeq());
        assertEquals(first.keptEvents(), store.readEvents(SYM, 0, 10_000).size(),
                "幂等压缩后事件集合保持不变");

        // 压缩后继续写入：新事件 seq 必须接着原最大序号，绝不复用被删号段
        long maxSeqBeforeWrite = store.maxSeq(SYM);
        send(7, 21_000, 16.0, 1);   // w21 OPEN，w15 结算
        send(8, 22_000, 17.0, 1);   // w22 OPEN，w16 结算
        List<CursorEvent> all = store.readEvents(SYM, 0, 10_000);
        long maxSeq = all.stream().mapToLong(CursorEvent::seq).max().orElseThrow();
        log.info("[TEST-ASSERT] 压缩后再写入，maxSeq {} -> {}", maxSeqBeforeWrite, maxSeq);
        assertTrue(maxSeq > maxSeqBeforeWrite, "新事件 seq 只增");
        Set<Long> seqs = new HashSet<>();
        all.forEach(e -> assertTrue(seqs.add(e.seq()), "seq 全局不得重复/复用: " + e.seq()));
        // 压缩前最大 seq 为 39；w21/w22 的新事件必须从 40 起，绝不复用被删号段 1..18
        List<CursorEvent> newWindowEvents = all.stream()
                .filter(e -> e.candle().windowStart() >= T0 + 21_000).toList();
        assertFalse(newWindowEvents.isEmpty(), "压缩后继续写入应产生 w21/w22 事件");
        assertTrue(newWindowEvents.stream().allMatch(e -> e.seq() > 39),
                "压缩后新事件只能接着 39 往后编号，不得复用删除号段 1..18");

        // 再压缩：幂等，且前沿不因新结算而后退；新结算窗口的折叠只增前沿
        CompactionReport fourth = store.compact(SYM);
        log.info("[TEST-ASSERT] 写入后再压缩 dropped={}, frontier={} -> {}",
                fourth.droppedEvents(), frontier, fourth.compactedThroughSeq());
        assertTrue(fourth.compactedThroughSeq() >= frontier, "前沿单调不减");
        assertEquals(0, store.compact(SYM).droppedEvents(), "再次压缩仍然幂等");

        // 从头重放始终收敛到区间查询
        assertEquals(snapshotView(), viewOf(drain(null, 16)),
                "反复压缩与继续写入后，重放视图仍须与区间查询一致");
    }

    // ---------------------------------------------------------------------
    // 持续写入 + 并发压缩 + 多客户端持续拉取
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("持续写入叠加并发压缩与多客户端拉取：按恢复协议重置后事件不重、seq只向前、视图与快照一致")
    void concurrentWriteCompactPull_staysConsistentWithSnapshot() throws Exception {
        int clients = 4;
        ExecutorService pool = Executors.newFixedThreadPool(2 + clients);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean writerDone = new AtomicBoolean(false);

        // 写入线程：确定性脚本，100 个窗口，含窗口内多笔与周期性迟到回补
        pool.submit(() -> {
            await(start);
            long id = 1;
            for (int w = 0; w < 100; w++) {
                ingestService.ingest(new TradeTick(SYM, id++, T0 + w * 1000L + 100, 10.0 + w, 1));
                ingestService.ingest(new TradeTick(SYM, id++, T0 + w * 1000L + 200, 11.0 + w, 2));
                if (w > 0 && w % 10 == 0) {
                    ingestService.ingest(new TradeTick(SYM, id++, T0 + (w - 1) * 1000L + 300, 9.0, 5));
                }
            }
            writerDone.set(true);
            return null;
        });

        // 压缩线程：先等写入产生足够多已结算窗口再开始，并在写入期间反复压缩
        pool.submit(() -> {
            await(start);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int rounds = 0;
            while (!writerDone.get()) {
                CompactionReport r = store.compact(SYM);
                rounds++;
                if (rounds <= 3 || rounds % 100 == 0) {
                    log.info("[TEST-INPUT] 并发压缩第 {} 轮 dropped={} frontier={}",
                            rounds, r.droppedEvents(), r.compactedThroughSeq());
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            CompactionReport fin = store.compact(SYM);
            log.info("[TEST-ASSERT] 并发压缩收尾，共 {} 轮, frontier={}", rounds, fin.compactedThroughSeq());
            return null;
        });

        // 多个客户端：持续分页；一旦收到 COMPACTED 就清空本地状态、改用 resumeCursor 重放。
        // 以“是否重置过”分段收集：每段内部事件不得重复、seq 严格递增。
        List<List<CursorEvent>> clientEvents = new ArrayList<>();
        List<Boolean> clientReset = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            clientEvents.add(new ArrayList<>());
            clientReset.add(false);
        }
        List<Future<?>> futures = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            final int clientId = c;
            futures.add(pool.submit(() -> {
                await(start);
                String cursor = null;
                long lastSeq = 0L;
                Set<Long> segmentSeen = new HashSet<>();
                int guard = 0;
                while (guard++ < 1_000_000) {
                    try {
                        CursorPage page = cursorService.pull(SYM, cursor, 3);
                        for (CursorEvent e : page.events()) {
                            assertTrue(e.seq() > lastSeq,
                                    "客户端" + clientId + " 段内 seq 必须严格递增: " + e.seq());
                            assertTrue(segmentSeen.add(e.seq()),
                                    "客户端" + clientId + " 段内不得收到重复 seq: " + e.seq());
                            lastSeq = e.seq();
                            clientEvents.get(clientId).add(e);
                        }
                        cursor = page.nextCursor();
                        if (!page.hasMore() && writerDone.get() && page.events().isEmpty()) {
                            break;
                        }
                        if (page.events().isEmpty()) {
                            Thread.sleep(2); // 等待写入或压缩推进，避免空转打满 CPU
                        }
                    } catch (CursorException ce) {
                        if (ce.reason() != CursorException.Reason.COMPACTED) {
                            throw ce;
                        }
                        // 恢复协议：丢弃本地增量视图，从 resumeCursor 干净重放
                        log.info("[TEST-DECISION] 客户端{} 游标被压缩(frontier)，重置后从 resumeSeq={} 重放",
                                clientId, ce.resumeSeq());
                        clientEvents.get(clientId).clear();
                        segmentSeen.clear();
                        lastSeq = 0L;
                        cursor = ce.resumeCursor();
                        clientReset.set(clientId, true);
                    }
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        Map<Long, Candle> snapshot = snapshotView();
        for (int c = 0; c < clients; c++) {
            List<CursorEvent> events = clientEvents.get(c);
            // 同一条事件（seq）在每个客户端的最终收集结果里至多出现一次，且整体只向前
            Set<Long> seqs = new HashSet<>();
            long prev = 0;
            for (CursorEvent e : events) {
                assertTrue(e.seq() > prev, "客户端" + c + " 最终 seq 必须只向前: " + e.seq());
                assertTrue(seqs.add(e.seq()), "客户端" + c + " 不得重复收到 seq: " + e.seq());
                prev = e.seq();
            }
            Map<Long, Candle> view = viewOf(events);
            log.info("[TEST-ASSERT] 客户端{} 触发重置={}, 最终事件数={}, 重建窗口数={}, 快照窗口数={}",
                    c, clientReset.get(c), events.size(), view.size(), snapshot.size());
            assertEquals(snapshot, view,
                    "客户端" + c + " 在持续写入+并发压缩下重建的窗口状态必须与区间查询完全一致");
        }
        // 至少有一个客户端真的撞上压缩并走了恢复路径，保证该分支被稳定覆盖
        assertTrue(clientReset.contains(true), "至少一个客户端应实际经历 COMPACTED 恢复");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
