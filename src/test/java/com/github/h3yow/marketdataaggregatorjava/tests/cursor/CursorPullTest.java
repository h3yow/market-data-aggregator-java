package com.github.h3yow.marketdataaggregatorjava.tests.cursor;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
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
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 游标增量拉取测试：幂等不重不漏、分页连续、以及 MALFORMED / BAD_SIGNATURE /
 * EXPIRED / SYMBOL_MISMATCH 四类拒绝原因可区分。
 */
class CursorPullTest {

    private static final Logger log = LoggerFactory.getLogger(CursorPullTest.class);

    private static final String SYM = "AU2312";
    private static final long T0 = 1_700_000_000_000L;

    private MarketDataProperties props;
    private AggregationStore store;
    private CursorService cursorService;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(60_000);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        cursorService = new CursorService(store, props);
        ingestService = new IngestService(store);
    }

    private void tick(long id, long offset) {
        TradeTick t = new TradeTick(SYM, id, T0 + offset, 10.0 + id, 1);
        log.info("[TEST-INPUT] {}", t);
        ingestService.ingest(t);
    }

    @Test
    @DisplayName("首次拉取(null游标)从 seq=0 开始；同一游标重复拉取结果完全一致(不重不漏)")
    void replaySameCursorYieldsIdenticalEvents() {
        tick(1, 100);
        tick(2, 3500); // 产生 w1/w2 缺口，事件数变多

        CursorPage first = cursorService.pull(SYM, null, 100);
        log.info("[TEST-ASSERT] 首次拉取 {} 条, seqs={}",
                first.events().size(), first.events().stream().map(CursorEvent::seq).toList());
        assertTrue(first.events().size() >= 4);
        long firstTotalVolumeSignature = first.events().size();

        CursorPage replay = cursorService.pull(SYM, null, 100);
        assertEquals(first.events().size(), replay.events().size());
        for (int i = 0; i < first.events().size(); i++) {
            assertEquals(first.events().get(i).seq(), replay.events().get(i).seq());
            assertEquals(first.events().get(i).type(), replay.events().get(i).type());
        }
        assertEquals(firstTotalVolumeSignature, replay.events().size());
    }

    @Test
    @DisplayName("透传 nextCursor 翻页：跨页 seq 严格递增不重叠不缺号，覆盖全部事件")
    void paginationWithNextCursorCoversAllEventsExactlyOnce() {
        for (long id = 1; id <= 5; id++) {
            tick(id, id * 2000); // 每笔跨窗口并带缺口，事件较多
        }
        long expectedTotal = store.maxSeq(SYM);

        List<Long> seenSeqs = new ArrayList<>();
        String cursor = null;
        int guard = 0;
        while (guard++ < 1000) {
            CursorPage page = cursorService.pull(SYM, cursor, 2); // 每页 2 条
            page.events().forEach(e -> seenSeqs.add(e.seq()));
            cursor = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        log.info("[TEST-ASSERT] 共收集 seqs={}", seenSeqs);
        assertEquals(expectedTotal, seenSeqs.size());
        for (int i = 0; i < seenSeqs.size(); i++) {
            assertEquals(i + 1L, seenSeqs.get(i), "seq 必须连续且每页不重叠");
        }
        // 末页之后再拉为空，hasMore=false，游标稳定
        CursorPage tail = cursorService.pull(SYM, cursor, 2);
        assertTrue(tail.events().isEmpty());
        assertFalse(tail.hasMore());
        assertEquals(cursor, tail.nextCursor());
    }

    @Test
    @DisplayName("回补后用旧游标增量拉取：只得到新的 CORRECTION 事件，已拉取事件不重复")
    void onlyCorrectionsArriveAfterBackfill() {
        tick(1, 100);
        tick(2, 3500);
        CursorPage before = cursorService.pull(SYM, null, 100);
        long seqAtCheckpoint = before.events().get(before.events().size() - 1).seq();

        tick(3, 1200); // 回补 w1 -> CORRECTION
        CursorPage after = cursorService.pull(SYM, before.nextCursor(), 100);
        log.info("[TEST-ASSERT] 回补后增量事件={}", after.events().stream().map(CursorEvent::type).toList());
        assertEquals(1, after.events().size());
        assertEquals(seqAtCheckpoint + 1, after.events().get(0).seq());
        assertEquals("CORRECTION", after.events().get(0).type().name());
    }

        @Test
    @DisplayName("MALFORMED：乱码/缺段/字段非数字游标被明确拒绝")
    void malformedCursorRejected() {
        assertMalformed("not-a-cursor");
        assertMalformed("abc");                 // 没有 '.'
        assertMalformed("abc.def.ghi");         // 多段
        // base64 可解码但签名不匹配：属于 BAD_SIGNATURE（安全校验先于结构解析）
        CursorException sigEx = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, b64("garbage") + "." + b64("sig"), 10));
        assertEquals(CursorException.Reason.BAD_SIGNATURE, sigEx.reason());
        // 签名正确但载荷字段数错误：应先通过签名校验再判 MALFORMED
        String payloadPart = b64("garbage");
        javax.crypto.Mac mac;
        String badFields;
        try {
            mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(
                    props.getCursorSecret().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    "HmacSHA256"));
            badFields = payloadPart + "." + Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(mac.doFinal(payloadPart.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, badFields, 10));
        assertEquals(CursorException.Reason.MALFORMED, ex.reason());
    }

    @Test
    @DisplayName("BAD_SIGNATURE：签名被篡改被拒绝，且与 MALFORMED 可区分")
    void tamperedSignatureRejected() {
        String good = cursorService.issue(SYM, 5L);
        int dot = good.indexOf('.');
        String tampered = good.substring(0, dot) + "." + b64("forged-signature");
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, tampered, 10));
        assertEquals(CursorException.Reason.BAD_SIGNATURE, ex.reason());
    }

    @Test
    @DisplayName("SYMBOL_MISMATCH：A 标的的游标用于 B 标的被拒绝")
    void cursorBoundToSymbol() {
        String cursor = cursorService.issue("AAA", 1L);
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull("BBB", cursor, 10));
        assertEquals(CursorException.Reason.SYMBOL_MISMATCH, ex.reason());
    }

    @Test
    @DisplayName("EXPIRED：超过 TTL 的游标被拒绝(410 语义)，与签名错误可区分")
    void expiredCursorRejected() throws Exception {
        props.setCursorTtlMillis(10);
        String cursor = cursorService.issue(SYM, 3L);
        Thread.sleep(30);
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, cursor, 10));
        assertEquals(CursorException.Reason.EXPIRED, ex.reason());
    }

    private void assertMalformed(String cursor) {
        CursorException ex = assertThrows(CursorException.class,
                () -> cursorService.pull(SYM, cursor, 10));
        assertEquals(CursorException.Reason.MALFORMED, ex.reason(),
                "游标 [" + cursor + "] 应判为 MALFORMED");
    }

    private static String b64(String s) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
