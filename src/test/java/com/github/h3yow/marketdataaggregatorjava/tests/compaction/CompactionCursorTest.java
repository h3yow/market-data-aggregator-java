package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionResult;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CompactionService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorException;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件流压缩/归档与游标兼容性测试。
 *
 * <p>场景：
 * <ol>
 *   <li>仅归档（archive）：归档段仍可回放，持有任意早期游标的客户端继续拉取，
 *       结果与未压缩时完全一致——不漏事件、不重复事件；</li>
 *   <li>归档后物理清除（trim）：早于热起点的游标判 UNAVAILABLE（410 语义），
 *       携带 recoverAfterSeq / recoverWindowStart / recoverCursor；
 *       用 recoverCursor 继续可拿到全部剩余事件，与未压缩时的后缀完全一致；</li>
 *   <li>压缩后继续写入，新事件序号接续、归档回放稳定。</li>
 * </ol>
 */
class CompactionCursorTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionCursorTest.class);

    private static final String SYM = "CP2312";
    private static final long T0 = 1_700_000_000_000L;

    private MarketDataProperties props;
    private AggregationStore store;
    private IngestService ingestService;
    private CursorService cursorService;
    private CompactionService compactionService;

    @BeforeEach
    void setUp() {
        props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(5000);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        ingestService = new IngestService(store);
        cursorService = new CursorService(store, props);
        compactionService = new CompactionService(store);
    }

    private void send(long id, long offset, double price) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, 1);
        log.info("[TEST-INPUT] {}", tick);
        ingestService.ingest(tick);
    }

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

    private List<Long> seqs(List<CursorEvent> events) {
        return events.stream().map(CursorEvent::seq).toList();
    }

    @Test
    @DisplayName("仅归档(不清除)：旧游标继续拉取与压缩前完全一致，事件不重不漏")
    void archivedEventsStillReplayableForOldCursors() {
        send(1, 0, 10);
        send(2, 6000, 11);   // w0 恰好结算（截止线 6000）
        send(3, 13_000, 12); // w0..w6 前缀陆续结算
        // 压缩前的“真值”：全量事件
        List<CursorEvent> baseline = store.readEvents(SYM, 0, 10_000);

        CompactionResult archived = compactionService.archiveSettled(SYM);
        log.info("[TEST-ASSERT] 归档结果 {}", archived);
        assertTrue(archived.archivedCount() > 0, "应有已结算事件被归档");
        assertEquals(0L, archived.hotBaseSeq(), "归档保留时逻辑热起点仍为 0");

        // 持有最早游标(seq=0)的客户端：归档后全量拉取必须与压缩前逐事件相等
        List<CursorEvent> afterArchive = pullAll(null, 2);
        assertEquals(seqs(baseline), seqs(afterArchive),
                "归档后全量重放的 seq 序列必须与压缩前一致");
        assertEquals(baseline, afterArchive, "归档后全量重放的事件内容必须与压缩前一致");

        // 持有归档内游标（如 seq=1）也必须只得到其后缀，且与压缩前后缀一致
        List<CursorEvent> suffixBefore = baseline.stream().filter(e -> e.seq() > 1).toList();
        List<CursorEvent> suffixAfter = pullAll(cursorService.issue(SYM, 1L), 3);
        assertEquals(suffixBefore, suffixAfter, "归档内旧游标的后缀必须不变");

        // 重复归档是幂等空操作，不丢不重
        CompactionResult again = compactionService.archiveSettled(SYM);
        assertEquals(0, again.archivedCount());
        assertEquals(seqs(baseline), seqs(pullAll(null, 5)),
                "重复归档后重放仍必须与压缩前一致");
    }

    @Test
    @DisplayName("物理清除后：旧游标判 UNAVAILABLE 并给出恢复起点；用 recoverCursor 继续拿到完整后缀")
    void trimmedArchiveMakesOldCursorUnavailableWithRecoveryPoint() {
        send(1, 0, 10);
        send(2, 6000, 11);
        send(3, 13_000, 12);
        List<CursorEvent> baseline = store.readEvents(SYM, 0, 10_000);

        CompactionResult trimmed = compactionService.trimArchived(SYM);
        log.info("[TEST-ASSERT] 物理清除结果 {}", trimmed);
        long hotBase = trimmed.hotBaseSeq();
        assertTrue(hotBase > 0);

        // 早于热起点的游标：UNAVAILABLE，与 EXPIRED/MALFORMED 可区分
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, cursorService.issue(SYM, 0L), 10));
        assertEquals(CursorException.Reason.UNAVAILABLE, ex.reason());
        assertNotNull(ex.recoverAfterSeq());
        assertNotNull(ex.recoverWindowStart());
        assertNotNull(ex.recoverCursor());
        log.info("[TEST-ASSERT] UNAVAILABLE recoverAfterSeq={}, window={}",
                ex.recoverAfterSeq(), ex.recoverWindowStart() - T0);
        assertEquals(hotBase, ex.recoverAfterSeq(), "恢复起点必须是热日志起点的前一个 seq");

        // 边界：seq == hotBase 仍可用（事件 seq > hotBase 都在热日志里）
        String boundaryCursor = cursorService.issue(SYM, hotBase);
        List<CursorEvent> fromRecovery = pullAll(boundaryCursor, 2);
        List<CursorEvent> expectedSuffix = baseline.stream()
                .filter(e -> e.seq() > hotBase).toList();
        assertEquals(seqs(expectedSuffix), seqs(fromRecovery),
                "从恢复起点继续必须拿到未压缩时的完整后缀");
        assertEquals(expectedSuffix, fromRecovery);

        // recoverCursor 同样可用且落在恢复起点
        List<CursorEvent> viaRecover = pullAll(ex.recoverCursor(), 4);
        assertEquals(seqs(expectedSuffix), seqs(viaRecover),
                "recoverCursor 继续拉取的后缀必须与未压缩一致");
    }

    @Test
    @DisplayName("同窗口事件跨归档/热段：w0 的 OPEN 已清除而 SETTLED 在热段，恢复后仍得到最终结算结论")
    void windowEventsMayStraddleArchiveAndHotButReplayStaysCorrect() {
        send(1, 0, 10);      // seq1: w0 OPEN
        send(2, 6000, 11);   // seq2..6 GAP(w1..w5), seq7 w6 OPEN, seq8 w0 SETTLED
        List<CursorEvent> baseline = store.readEvents(SYM, 0, 10_000);

        // 归档：只有 seq1 所属的 w0 在扫描点“已结算”，seq2 的 w1 缺口未结算 -> 仅归档 seq1
        CompactionResult archived = compactionService.archiveSettled(SYM);
        assertEquals(1, archived.archivedCount());
        assertEquals(1L, archived.archivedToSeq());

        // 物理清除后：恢复起点 seq=1，恢复后缀 seq2..8 中仍包含 w0 的 SETTLED(seq8)
        compactionService.trimArchived(SYM);
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, cursorService.issue(SYM, 0L), 10));
        assertEquals(CursorException.Reason.UNAVAILABLE, ex.reason());
        List<CursorEvent> suffix = pullAll(ex.recoverCursor(), 3);
        assertEquals(seqs(baseline.stream().filter(e -> e.seq() > 1).toList()),
                seqs(suffix), "恢复后缀必须等于未压缩时的 seq2..max");
        CursorEvent w0Settled = suffix.stream()
                .filter(e -> e.type() == com.github.h3yow.marketdataaggregatorjava.domain.EventType.SETTLED
                        && e.candle().windowStart() == T0)
                .findFirst().orElseThrow();
        assertTrue(w0Settled.candle().settled(),
                "即使 w0 的 OPEN 已被物理清除，恢复后缀仍必须携带其 SETTLED 最终结论");
    }

    @Test
    @DisplayName("压缩后继续写入：新事件序号接续；归档游标与热游标都不重不漏")
    void writesAfterCompactionContinueSeamlessly() {
        send(1, 0, 10);
        send(2, 6000, 11);
        compactionService.trimArchived(SYM); // 清掉 w0 的已结算前缀
        long hotBase = store.trimmedBaseSeq(SYM);

        // 清除后继续写入（新的结算窗口 + 新事件）
        send(4, 13_000, 12);
        send(5, 20_000, 13);

        // 未受影响的合法游标（seq >= hotBase）继续拉取：得到全部后续事件一次
        List<CursorEvent> tail = pullAll(cursorService.issue(SYM, hotBase), 2);
        long maxSeq = store.maxSeq(SYM);
        List<Long> tailSeqs = seqs(tail);
        log.info("[TEST-ASSERT] hotBase={}, maxSeq={}, tailSeqs={}", hotBase, maxSeq, tailSeqs);
        assertEquals(maxSeq - hotBase, tailSeqs.size(), "后缀事件条数必须连续");
        for (int i = 0; i < tailSeqs.size(); i++) {
            assertEquals(hotBase + 1 + i, tailSeqs.get(i), "新事件序号必须接续不复用");
        }
        // 再次归档：新结算的事件进入归档，旧游标回放仍不重不漏
        CompactionResult more = compactionService.archiveSettled(SYM);
        log.info("[TEST-ASSERT] 二次归档 {}", more);
        List<CursorEvent> tailAgain = pullAll(cursorService.issue(SYM, hotBase), 2);
        assertEquals(tailSeqs, seqs(tailAgain), "二次归档后后缀回放必须不变");
    }
}
