package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
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

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 压缩恢复游标的安全与失效语义：
 * 恢复授权段受同一 HMAC 保护，不可伪造/篡改；恢复期间若又发生压缩（水位越过授权），
 * 旧恢复游标失效并返回携带更高水位的新恢复指引。
 */
class RecoveryCursorSecurityTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryCursorSecurityTest.class);
    private static final String SYM = "RCSEC";
    private static final long T0 = 1_700_000_000_000L;

    private MarketDataProperties props;
    private AggregationStore store;
    private CursorService cursors;
    private IngestService ingest;

    @BeforeEach
    void setUp() {
        props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(5000);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        cursors = new CursorService(store, props);
        ingest = new IngestService(store);
    }

    private void prime() {
        ingest.ingest(new TradeTick(SYM, 1, T0 + 100, 10, 1));
        ingest.ingest(new TradeTick(SYM, 2, T0 + 200, 11, 2));
        ingest.ingest(new TradeTick(SYM, 3, T0 + 3500, 12, 3));
        ingest.ingest(new TradeTick(SYM, 4, T0 + 20_000, 15, 5));
    }

    private String recoverCursor() {
        CursorException ex = assertThrows(CursorException.class,
                () -> cursors.pull(SYM, cursors.issue(SYM, 2), 2));
        assertEquals(CursorException.Reason.COMPACTED, ex.reason());
        return ex.recovery().fullReplayCursor();
    }

    private String tamperPayload(String cursor) {
        Base64.Decoder dec = Base64.getUrlDecoder();
        String[] parts = cursor.split("\\.", 2);
        String payload = new String(dec.decode(parts[0]), java.nio.charset.StandardCharsets.UTF_8);
        // 把授权水位改大
        String tampered = payload.replaceAll("R[0-9]+", "R999999");
        String payloadPart = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(tampered.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return payloadPart + "." + parts[1];
    }

    @Test
    @DisplayName("篡改恢复授权段（伪造更大水位）必须 BAD_SIGNATURE 拒绝")
    void tamperedRecoveryCursorRejected() {
        prime();
        store.compact(SYM);
        String recovery = recoverCursor();
        String tampered = tamperPayload(recovery);
        CursorException ex = assertThrows(CursorException.class,
                () -> cursors.pull(SYM, tampered, 2));
        log.info("[TEST-ASSERT] 篡改恢复游标 -> reason={}", ex.reason());
        assertEquals(CursorException.Reason.BAD_SIGNATURE, ex.reason(),
                "恢复授权段受 HMAC 保护，篡改后必须签名校验失败");
    }

    @Test
    @DisplayName("客户端自行给普通游标追加 R 段必须 BAD_SIGNATURE 拒绝（授权不可伪造）")
    void forgedRecoverySuffixRejected() {
        prime();
        store.compact(SYM);
        long frontier = store.compactedThroughSeq(SYM);
        String normal = cursors.issue(SYM, 2); // 2 < frontier，普通游标本会被 COMPACTED 拒绝
        String[] parts = normal.split("\\.", 2);
        String payload = new String(Base64.getUrlDecoder().decode(parts[0]),
                java.nio.charset.StandardCharsets.UTF_8);
        String forgedPayload = payload + "|R" + frontier;
        String forged = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(forgedPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                + "." + parts[1];
        CursorException ex = assertThrows(CursorException.class,
                () -> cursors.pull(SYM, forged, 2));
        log.info("[TEST-ASSERT] 伪造 R 授权段 -> reason={}", ex.reason());
        assertEquals(CursorException.Reason.BAD_SIGNATURE, ex.reason());
    }

    @Test
    @DisplayName("恢复期间又发生压缩使水位越过授权：尚停在折叠区间的恢复游标失效，重新 COMPACTED 并给出更高水位")
    void recoveryCursorInvalidatedByNewerCompaction() {
        prime();
        store.compact(SYM);
        long frontier1 = store.compactedThroughSeq(SYM);
        String recoveryStart = recoverCursor();
        log.info("[TEST-ASSERT] 首次恢复游标授权水位={}", frontier1);

        // 用恢复游标读一页，客户端停在日志中的某个位置（仍 < 后续新水位）
        CursorPage page1 = cursors.pull(SYM, recoveryStart, 2);
        String paused = page1.nextCursor();
        long pausedSeq = page1.events().get(page1.events().size() - 1).seq();
        log.info("[TEST-ASSERT] 恢复回放暂停于 seq={}", pausedSeq);

        // 继续写入推进更多窗口结算，再压缩，使压缩水位越过暂停位置
        ingest.ingest(new TradeTick(SYM, 5, T0 + 40_000, 20, 1));
        store.compact(SYM);
        long frontier2 = store.compactedThroughSeq(SYM);
        assertTrue(frontier2 > frontier1, "新一轮压缩必须把水位向前推进: " + frontier1 + " -> " + frontier2);
        assertTrue(pausedSeq < frontier2, "测试前提：暂停位置必须落在新折叠区间内");

        CursorException ex = assertThrows(CursorException.class,
                () -> cursors.pull(SYM, paused, 2));
        log.info("[TEST-ASSERT] 旧恢复游标在新水位 {} 下 -> reason={}", frontier2, ex.reason());
        assertEquals(CursorException.Reason.COMPACTED, ex.reason());
        assertNotNull(ex.recovery());
        assertEquals(frontier2, ex.recovery().compactedThroughSeq(),
                "新恢复指引必须携带更新后的水位");
        // 用新恢复游标可以继续（不再抛异常）
        CursorPage page = cursors.pull(SYM, ex.recovery().fullReplayCursor(), 2);
        assertNotNull(page);
    }
}
