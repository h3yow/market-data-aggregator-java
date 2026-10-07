package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.github.h3yow.marketdataaggregatorjava.service.CursorException;
import com.github.h3yow.marketdataaggregatorjava.web.GlobalExceptionHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * COMPACTED 拒绝在 HTTP 层的契约：410 Gone，body 可区分原因并携带
 * 可恢复起点 resumeSeq 与新游标 resumeCursor；其它游标错误仍为 400/410 且不带恢复字段。
 */
class CompactionCursorHttpErrorTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionCursorHttpErrorTest.class);

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    @DisplayName("COMPACTED -> 410，body 含 reason/message/resumeSeq/resumeCursor")
    void compactedReturns410WithRecoveryCursor() {
        CursorException ex = new CursorException(CursorException.Reason.COMPACTED,
                "游标续拉位置 seq=3 所在事件已被压缩删除", 18L, "NEW-RESUME-CURSOR");

        ResponseEntity<Map<String, Object>> resp = handler.handleCursor(ex);
        log.info("[TEST-ASSERT] COMPACTED HTTP 响应 status={}, body={}", resp.getStatusCode(), resp.getBody());

        assertEquals(HttpStatus.GONE, resp.getStatusCode(), "压缩失效必须与正常拉取区分，使用 410 Gone");
        Map<String, Object> body = resp.getBody();
        assertNotNull(body);
        assertEquals("INVALID_CURSOR", body.get("error"));
        assertEquals("COMPACTED", body.get("reason"));
        assertEquals(18L, body.get("resumeSeq"), "恢复起点必须随响应返回");
        assertEquals("NEW-RESUME-CURSOR", body.get("resumeCursor"), "新游标必须随响应返回");
        assertTrue(((String) body.get("message")).contains("压缩"), "message 必须说明不可用原因");
    }

    @Test
    @DisplayName("MALFORMED 仍为 400，且不携带 resume 字段")
    void malformedStays400WithoutResume() {
        ResponseEntity<Map<String, Object>> resp = handler.handleCursor(
                new CursorException(CursorException.Reason.MALFORMED, "格式错误"));
        log.info("[TEST-ASSERT] MALFORMED HTTP 响应 status={}, body={}", resp.getStatusCode(), resp.getBody());
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals("MALFORMED", resp.getBody().get("reason"));
        assertFalse(resp.getBody().containsKey("resumeCursor"));
    }

    private static void assertFalse(boolean actual) {
        org.junit.jupiter.api.Assertions.assertFalse(actual);
    }
}
