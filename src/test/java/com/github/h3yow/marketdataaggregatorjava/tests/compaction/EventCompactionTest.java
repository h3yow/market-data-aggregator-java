package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.EventType;
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
 * <p>压缩规则：已结算窗口只保留 WINDOW_SETTLED 事件（最终快照），seq 不重编号。
 * 游标兼容范围只有两种、且都必须被显式断言：
 * <ol>
 *   <li><b>逐条一致路径</b>：游标 seq 不小于压缩水位（其后续拉区间没有事件被折叠），
 *       压缩前后用同一游标拉到的事件序列逐条相同，不漏不重；</li>
 *   <li><b>明确不可用 + 恢复起点路径</b>：游标 seq 位于压缩水位之前，
 *       拉取以 {@link CursorException.Reason#COMPACTED} 被明确拒绝（不静默返回改写序列），
 *       异常携带恢复起点与新游标；客户端按恢复游标继续后，重建窗口与区间快照查询一致。</li>
 * </ol>
 * 另覆盖：反复压缩幂等且压缩水位只增不减、压缩后新事件 seq 不复用；
 * 持续写入叠加反复压缩与多个持续拉取客户端时，每个客户端不重不漏、
 * 收到的 seq 只向前走，最终重建视图与区间快照完全一致。
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

    /** 为两套独立 store 灌入同一条确定性脚本，保证输入完全一致。 */
    private AggregationStore newStoreWithScript() {
        MarketDataProperties p = new MarketDataProperties();
        p.setWindowSizeMillis(W);
        p.setOutOfOrderToleranceMillis(TOL);
        p.setCursorTtlMillis(3_600_000);
        AggregationStore s = new AggregationStore(p);
        IngestService ing = new IngestService(s);
        ing.ingest(new TradeTick(SYM, 1, T0 + 100, 10.0, 1));
        ing.ingest(new TradeTick(SYM, 2, T0 + 200, 11.0, 2));
        ing.ingest(new TradeTick(SYM, 3, T0 + 300, 9.0, 1));
        ing.ingest(new TradeTick(SYM, 4, T0 + 3500, 12.0, 3));
        ing.ingest(new TradeTick(SYM, 5, T0 + 1200, 10.5, 7));
        ing.ingest(new TradeTick(SYM, 6, T0 + 20_000, 15.0, 5));
        return s;
    }

    private CursorService cursorsFor(AggregationStore s) {
        return new CursorService(s, props);
    }

    /** 用给定游标把剩余事件全部拉完（小分页，模拟真实分页），返回收到的事件。 */
    private List<CursorEvent> pullAll(CursorService svc, String cursor) {
        List<CursorEvent> out = new ArrayList<>();
        String next = cursor;
        int guard = 0;
        while (guard++ < 10_000) {
            CursorPage page = svc.pull(SYM, next, 2);
            out.addAll(page.events());
            next = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        return out;
    }

    /** 客户端视图：同窗口后到的快照覆盖先到的。 */
    private Map<Long, Candle> viewOf(List<CursorEvent> events) {
        Map<Long, Candle> view = new HashMap<>();
        events.forEach(e -> view.put(e.candle().windowStart(), e.candle()));
        return view;
    }

    private Map<Long, Candle> replayFrom(String cursor) {
        return viewOf(pullAll(cursorService, cursor));
    }

    private Map<Long, Candle> snapshotView() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    private String cursorAt(long seq) {
        return cursorService.issue(SYM, seq);
    }

    // ---------- 用例 1：压缩后从头重放（seq=0 永远可用）与快照一致 ----------

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
        log.info("[TEST-ASSERT] 判定依据：seq=0 从头拉取不受压缩水位限制；"
                + "压缩前视图窗口数={}, 压缩后={}", beforeView.size(), afterView.size());
        assertEquals(beforeView, afterView, "压缩后从头重放的视图必须与压缩前完全一致");
        assertEquals(snapshotView(), afterView, "压缩后重放视图必须与快照查询一致");
    }

    // ---------- 用例 2：逐条一致路径（安全旧游标压缩前后逐条相同） ----------

    @Test
    @DisplayName("逐条一致路径：游标 seq 不小于压缩水位时，压缩前后同游标拉取逐条相同不漏不重")
    void safeCursorPullsIdenticalSequenceBeforeAndAfterCompaction() {
        AggregationStore baseline = newStoreWithScript();
        CursorService baseCursors = cursorsFor(baseline);

        ingestScript();
        CompactionReport report = store.compact(SYM);
        long frontier = store.compactedThroughSeq(SYM);
        long maxSeq = store.maxSeq(SYM);
        log.info("[TEST-ASSERT] 压缩水位 frontier={}, maxSeq={}, dropped={}",
                frontier, maxSeq, report.droppedEvents());
        assertTrue(frontier > 0, "压缩应推进水位");
        assertTrue(frontier < maxSeq, "脚本应保留压缩水位之后的事件（本路径的判定依据）");

        // 分别取“恰好等于水位”和“严格大于水位”两个游标：边界与一般情况都断言
        for (long cursorSeq : List.of(frontier, frontier + 1)) {
            String cursor = cursorAt(cursorSeq);

            // 基线“未压缩时”客户端从 cursorSeq 续拉能得到的增量（事件本身 + 窗口集合）
            List<CursorEvent> expected = new ArrayList<>();
            for (CursorEvent e : baseline.readEvents(SYM, 0, 10_000)) {
                if (e.seq() > cursorSeq) {
                    expected.add(e);
                }
            }
            // 压缩后的实际拉取结果
            List<CursorEvent> actual = pullAll(cursorService, cursor);

            log.info("[TEST-ASSERT] 判定依据：游标 seq={} >= frontier={}，"
                            + "续拉区间内无被折叠事件；期望增量 seqs={}，实际增量 seqs={}",
                    cursorSeq, frontier, seqsOf(expected), seqsOf(actual));

            assertEquals(expected.size(), actual.size(),
                    "游标 seq=" + cursorSeq + " 压缩前后事件条数必须一致");
            Set<Long> unique = new HashSet<>();
            long prev = cursorSeq;
            for (int i = 0; i < expected.size(); i++) {
                CursorEvent a = expected.get(i);
                CursorEvent b = actual.get(i);
                assertEquals(a, b, "游标 seq=" + cursorSeq + " 压缩前后第 " + i + " 条事件必须逐条一致");
                assertTrue(b.seq() > prev, "seq 只能向前走: " + b.seq());
                assertTrue(unique.add(b.seq()), "seq 不得重复: " + b.seq());
                prev = b.seq();
            }
            // 模拟真实客户端：压缩前的存量视图（基线全量重放）+ 本次增量，结果必须等于快照
            Map<Long, Candle> merged = new HashMap<>();
            baseline.readEvents(SYM, 0, 10_000).stream()
                    .filter(e -> e.seq() <= cursorSeq)
                    .forEach(e -> merged.put(e.candle().windowStart(), e.candle()));
            actual.forEach(e -> merged.put(e.candle().windowStart(), e.candle()));
            assertEquals(snapshotView(), merged,
                    "安全游标：存量视图叠加压缩后增量必须与快照一致");
            // 增量自身涉及的窗口集合也必须与未压缩时一致（不漏窗口、不凭空多窗口）
            assertEquals(viewOf(expected).keySet(), viewOf(actual).keySet(),
                    "游标 seq=" + cursorSeq + " 压缩前后增量涉及的窗口集合必须一致");
        }
    }

    // ---------- 用例 3：明确不可用 + 恢复起点路径 ----------

    @Test
    @DisplayName("明确不可用路径：游标续拉位置被折叠时以 COMPACTED 拒绝并给出恢复起点，恢复后视图与快照一致")
    void staleCursorIsRejectedWithRecoveryAndRecoversToSnapshot() {
        ingestScript();
        // 客户端只消费前 2 条事件，手里拿着压缩之前的旧游标
        CursorPage firstPage = cursorService.pull(SYM, null, 2);
        long cursorSeq = firstPage.events().get(firstPage.events().size() - 1).seq();
        String oldCursor = firstPage.nextCursor();
        Map<Long, Candle> clientView = new HashMap<>();
        Set<Long> seenSeqs = new HashSet<>();
        firstPage.events().forEach(e -> {
            clientView.put(e.candle().windowStart(), e.candle());
            seenSeqs.add(e.seq());
        });

        CompactionReport report = store.compact(SYM);
        long frontier = store.compactedThroughSeq(SYM);
        log.info("[TEST-ASSERT] 旧游标 seq={} < 压缩水位 {}（折叠最大 seq={}），判定为不可逐条一致",
                cursorSeq, frontier, report.maxDroppedSeq());
        assertTrue(cursorSeq < frontier, "测试前提：游标续拉位置之后必须确有事件被折叠");

        // 用旧游标续拉：必须被明确拒绝，而不是静默返回改写后的序列
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, oldCursor, 2));
        assertEquals(CursorException.Reason.COMPACTED, ex.reason());
        CursorException.Recovery recovery = ex.recovery();
        log.info("[TEST-EXCEPTION] reason={}, message={}", ex.reason(), ex.getMessage());
        assertNotNull(recovery, "COMPACTED 拒绝必须携带恢复信息");
        assertEquals(cursorSeq, recovery.cursorSeq());
        assertEquals(frontier, recovery.compactedThroughSeq());
        // 恢复起点①继续向前：从压缩水位 frontier 开始；恢复起点②全量重建：从 seq=0 开始
        long earliest = store.earliestRetainedSeq(SYM);
        assertEquals(frontier, recovery.resumeFromSeq(),
                "继续向前的恢复起点必须提到压缩水位，否则会再次落入折叠区间");
        assertEquals(0L, recovery.replayFromSeq(), "全量重建游标必须从 seq=0 开始");
        assertNotNull(recovery.resumeCursor());
        assertNotNull(recovery.fullReplayCursor());
        log.info("[TEST-ASSERT] 恢复信息：resumeFromSeq={}, replayFromSeq={}, earliestRetained={}",
                recovery.resumeFromSeq(), recovery.replayFromSeq(), earliest);

        // 路径 A：用 resumeCursor 从压缩水位继续向前——不回退、不重号。
        // 折叠区间 (cursorSeq, frontier] 内窗口的最终状态用区间 K 线补齐；二者合并后等于快照。
        List<CursorEvent> resumed = pullAll(cursorService, recovery.resumeCursor());
        List<Long> resumedSeqs = new ArrayList<>();
        for (CursorEvent e : resumed) {
            assertTrue(e.seq() > frontier, "从压缩水位续拉只能收到比水位更晚的 seq: " + e.seq());
            assertTrue(seenSeqs.add(e.seq()), "同一 seq 不得重复下发: " + e.seq());
            resumedSeqs.add(e.seq());
            clientView.put(e.candle().windowStart(), e.candle());
        }
        log.info("[TEST-ASSERT] 路径A resumeCursor 从水位 {} 追加收到 seqs={}", frontier, resumedSeqs);
        // 用区间 K 线查询补齐折叠区间内已结算窗口的最终状态（文档承诺的恢复姿势）
        for (Candle c : store.snapshots(SYM, null, null)) {
            clientView.putIfAbsent(c.windowStart(), c);
        }
        assertEquals(snapshotView(), clientView,
                "从水位继续 + K线补齐折叠区间后，客户端视图必须与快照一致");

        // 路径 B：用 fullReplayCursor 全量重建——视图必须与快照一致，含每个已结算窗口结算事件，
        // 且重建流内部 seq 单调不重复（即使结算事件的 seq 早于压缩水位也必须能被授权回放）
        List<CursorEvent> full = pullAll(cursorService, recovery.fullReplayCursor());
        Set<Long> fullSeqs = new HashSet<>();
        long prev = 0;
        for (CursorEvent e : full) {
            assertTrue(e.seq() > prev, "全量重建事件 seq 必须严格递增: " + e.seq());
            assertTrue(fullSeqs.add(e.seq()), "全量重建不得重复 seq: " + e.seq());
            prev = e.seq();
        }
        Map<Long, Candle> rebuilt = viewOf(full);
        log.info("[TEST-ASSERT] 路径B fullReplayCursor 重建事件数={}, 窗口数={}, 快照窗口数={}",
                full.size(), rebuilt.size(), snapshotView().size());
        assertEquals(snapshotView(), rebuilt, "全量重建视图必须与快照一致");
        long settledCount = full.stream().filter(e -> e.type() == EventType.WINDOW_SETTLED).count();
        assertEquals(report.settledWindows(), settledCount,
                "每个已结算窗口的结算事件必须仍可从全量重建游标拉到");
        assertTrue(full.stream().anyMatch(e -> e.type() == EventType.WINDOW_SETTLED
                        && e.seq() > frontier),
                "判定依据：折叠前保留的结算事件 seq 会晚于水位（水位折叠的是更早的 OPEN/UPDATE），"
                        + "恢复授权必须能读到它们，否则全量重建会丢已结算窗口");
        // 这些“晚于水位的保留事件”对一个从中间开始的普通游标是会被挡住的：
        // 用 seq=0 的普通游标能读（从头永远可用），但 seq 在 (0,frontier) 的普通游标必被拒；
        // 恢复游标正是为了让“被告知过折叠”的客户端完成同样的全量重建。
        assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, cursorAt(frontier - 1), 100),
                "普通中间游标落在水位之前仍必须被拒绝（恢复授权不可伪造）");
    }

    /** 恰好位于压缩水位的游标是边界安全点：已消费到该 seq，其后事件全部保留，不得拒绝。 */
    @Test
    @DisplayName("边界：游标 seq 恰好等于压缩水位时继续逐条可用，不因压缩而被拒绝")
    void cursorExactlyAtFrontierRemainsUsable() {
        ingestScript();
        store.compact(SYM);
        long frontier = store.compactedThroughSeq(SYM);
        String cursor = cursorAt(frontier);
        List<CursorEvent> events = cursorService.pull(SYM, cursor, 100).events();
        log.info("[TEST-ASSERT] 判定依据：seq={} == frontier，已消费折叠位置，之后事件全保留；收到 {} 条",
                frontier, events.size());
        assertFalse(events.isEmpty(), "水位之后仍有保留事件（未结算窗口）");
        assertTrue(events.stream().allMatch(e -> e.seq() > frontier));
        // 这里是“从水位开始的增量”，与全量快照比较时需叠加水位处的存量视图
        Map<Long, Candle> merged = new HashMap<>();
        store.readEvents(SYM, 0, 10_000).stream()
                .filter(e -> e.seq() <= frontier)
                .forEach(e -> merged.put(e.candle().windowStart(), e.candle()));
        events.forEach(e -> merged.put(e.candle().windowStart(), e.candle()));
        assertEquals(snapshotView(), merged, "水位游标增量叠加存量视图必须与快照一致");
    }

    // ---------- 用例 4：反复压缩幂等、水位只增不减、seq 不复用 ----------

    @Test
    @DisplayName("反复压缩幂等：重复压缩不删事件、水位只增不减；压缩后继续写入 seq 不复用且重放一致")
    void compactionIdempotentAndSeqNeverReused() {
        ingestScript();
        CompactionReport first = store.compact(SYM);
        long frontier1 = store.compactedThroughSeq(SYM);
        assertEquals(first.maxDroppedSeq(), frontier1);

        CompactionReport second = store.compact(SYM);
        log.info("[TEST-ASSERT] 第二次压缩报告={}, 水位 {}->{}", second, frontier1,
                store.compactedThroughSeq(SYM));
        assertEquals(0, second.droppedEvents(), "已压缩过的日志再次压缩不得删除任何事件");
        assertEquals(frontier1, store.compactedThroughSeq(SYM), "重复压缩水位必须保持不变");

        long maxSeqAfterCompact = store.maxSeq(SYM);

        // 压缩后继续写入：新事件 seq 必须接着原有序号，绝不复用被删除的号段
        send(7, 21_000, 16.0, 1);   // w21，w15 结算
        send(8, 22_000, 17.0, 1);   // w22，w16 结算
        List<CursorEvent> all = store.readEvents(SYM, 0, 10_000);
        long maxSeq = all.stream().mapToLong(CursorEvent::seq).max().orElse(0);
        assertTrue(maxSeq > maxSeqAfterCompact, "压缩后新事件 seq 必须继续递增");
        assertEquals(maxSeq, store.maxSeq(SYM));
        Set<Long> seqs = new HashSet<>();
        all.forEach(e -> assertTrue(seqs.add(e.seq()), "seq 不得重复/复用: " + e.seq()));

        // 再次压缩：只折叠本轮新结算窗口，水位只增不减；之后再压一次幂等
        CompactionReport third = store.compact(SYM);
        long frontier3 = store.compactedThroughSeq(SYM);
        log.info("[TEST-ASSERT] 第三次压缩 dropped={}, 水位 {}->{}", third.droppedEvents(),
                frontier1, frontier3);
        assertTrue(frontier3 >= frontier1, "多轮压缩水位只能向前");
        assertTrue(third.droppedEvents() > 0, "新结算窗口的历史事件应在本轮被折叠");
        CompactionReport fourth = store.compact(SYM);
        assertEquals(0, fourth.droppedEvents(), "紧跟其后的重复压缩必须幂等");
        assertEquals(frontier3, store.compactedThroughSeq(SYM));

        assertEquals(snapshotView(), replayFrom(null), "压缩后继续写入，从头重放仍须与快照一致");
    }

    // ---------- 用例 5：持续写入 + 反复压缩 + 多个持续拉取客户端 ----------

    @Test
    @DisplayName("持续写入+反复压缩+多客户端持续拉取：各客户端不重不漏、seq 只向前，最终视图与快照一致")
    void concurrentWriteCompactAndPullStaysConsistent() throws Exception {
        int pullers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(2 + pullers);
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
            return null;
        });

        // 压缩线程：写入期间反复压缩（制造“游标位置被折叠”的拒绝窗口）
        pool.submit(() -> {
            await(start);
            while (!writerDone.get()) {
                store.compact(SYM);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            store.compact(SYM);
            return null;
        });

        // 多个拉取客户端：各自分页、收到 COMPACTED 就丢弃本地状态并用 fullReplayCursor 全量重建。
        // 重建本身仍要满足：每条事件只处理一次、seq 单调；重建次数不设上限（压缩可能反复发生）。
        List<List<CursorEvent>> collected = new ArrayList<>();
        List<AtomicBoolean> rejected = new ArrayList<>();
        for (int i = 0; i < pullers; i++) {
            collected.add(new ArrayList<>());
            rejected.add(new AtomicBoolean(false));
        }
        List<Future<?>> pullerFutures = new ArrayList<>();
        for (int p = 0; p < pullers; p++) {
            final int idx = p;
            pullerFutures.add(pool.submit(() -> {
                await(start);
                String cursor = null;
                List<CursorEvent> mine = collected.get(idx);
                int guard = 0;
                while (guard++ < 1_000_000) {
                    CursorPage page;
                    try {
                        page = cursorService.pull(SYM, cursor, 3);
                    } catch (CursorException e) {
                        // 唯一允许的拒绝原因：压缩折叠；恢复信息必须完整
                        assertEquals(CursorException.Reason.COMPACTED, e.reason());
                        CursorException.Recovery rec = e.recovery();
                        assertNotNull(rec);
                        assertTrue(rec.resumeFromSeq() > 0, "继续向前的恢复起点应为压缩水位");
                        assertNotNull(rec.resumeCursor());
                        assertNotNull(rec.fullReplayCursor());
                        rejected.get(idx).set(true);
                        // 选择全量重建路径：丢弃本地状态，用带授权的从头游标重放，
                        // 保证每个已结算窗口（含结算事件 seq 早于水位的窗口）都不丢。
                        synchronized (mine) {
                            mine.clear();
                        }
                        cursor = rec.fullReplayCursor();
                        continue;
                    }
                    synchronized (mine) {
                        long prev = mine.isEmpty() ? 0L : mine.get(mine.size() - 1).seq();
                        for (CursorEvent e : page.events()) {
                            assertTrue(e.seq() > prev,
                                    "客户端 " + idx + " seq 必须只向前走: " + e.seq());
                            mine.add(e);
                            prev = e.seq();
                        }
                    }
                    cursor = page.nextCursor();
                    // 停机必须同时满足：本页为空、已无更多、且写入线程确实结束。
                    // 仅在写入过程中临时追平（hasMore=false）不能退出，否则会漏掉后续事件。
                    if (page.events().isEmpty() && !page.hasMore() && writerDone.get()) {
                        break;
                    }
                }
                return null;
            }));
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发场景必须在超时前收敛");
        for (Future<?> f : pullerFutures) {
            f.get(5, TimeUnit.SECONDS); // 让拉取线程内的断言异常冒泡
        }

        Map<Long, Candle> snapshot = snapshotView();
        log.info("[TEST-ASSERT] 最终快照窗口数={}", snapshot.size());
        for (int p = 0; p < pullers; p++) {
            List<CursorEvent> events;
            synchronized (collected.get(p)) {
                events = List.copyOf(collected.get(p));
            }
            Set<Long> seqs = new HashSet<>();
            long prev = 0;
            for (CursorEvent e : events) {
                assertTrue(e.seq() > prev, "客户端 " + p + " 最终事件 seq 必须严格递增: " + e.seq());
                assertTrue(seqs.add(e.seq()), "客户端 " + p + " 不得收到重复 seq: " + e.seq());
                prev = e.seq();
            }
            Map<Long, Candle> view = viewOf(events);
            // 先输出差异窗口，便于判定是“缺窗口/多窗口”还是“同窗口快照不一致”
            Set<Long> missing = new java.util.TreeSet<>(snapshot.keySet());
            missing.removeAll(view.keySet());
            Set<Long> extra = new java.util.TreeSet<>(view.keySet());
            extra.removeAll(snapshot.keySet());
            Set<Long> common = new java.util.TreeSet<>(snapshot.keySet());
            common.retainAll(view.keySet());
            List<Long> differing = common.stream()
                    .filter(w -> !snapshot.get(w).equals(view.get(w)))
                    .map(w -> w - T0)
                    .toList();
            log.info("[TEST-ASSERT] 客户端{} 收到事件数={}, 重建窗口数={}, 是否经历过 COMPACTED 恢复={}, "
                            + "缺窗口={}, 多窗口={}, 内容不一致窗口={}",
                    p, events.size(), view.size(), rejected.get(p).get(),
                    missing.stream().map(w -> (w - T0)).toList(),
                    extra.stream().map(w -> (w - T0)).toList(),
                    differing.stream().map(w -> (w - T0)).toList());
            assertEquals(snapshot, view, "客户端 " + p + " 重建视图必须与区间快照查询一致");
        }
        // 至少有一个客户端在持续压缩过程中真实经历过“压缩拒绝→恢复”，否则该并发用例没有覆盖到关键路径
        long anyRecovered = rejected.stream().filter(AtomicBoolean::get).count();
        log.info("[TEST-ASSERT] 经历 COMPACTED 恢复的客户端数={}/{}", anyRecovered, pullers);
        assertTrue(anyRecovered >= 1, "并发用例应至少有一个客户端真实经历压缩拒绝并恢复");
    }

    private static List<Long> seqsOf(List<CursorEvent> events) {
        return events.stream().map(CursorEvent::seq).toList();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
