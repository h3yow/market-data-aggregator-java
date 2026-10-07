package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.service.CursorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一异常处理。
 *
 * <p>非法游标按 {@link CursorException.Reason} 返回可区分错误码：
 * <ul>
 *   <li>MALFORMED / BAD_SIGNATURE / SYMBOL_MISMATCH -> 400；</li>
 *   <li>EXPIRED（超 TTL）/ COMPACTED（续拉位置已被压缩）-> 410 Gone。</li>
 * </ul>
 * COMPACTED 额外携带 {@code resumeSeq}（压缩后日志首条保留事件 seq 减 1）
 * 与 {@code resumeCursor}：客户端清掉本地增量状态后用 resumeCursor 重放，
 * 即可无损恢复最终视图。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CursorException.class)
    public ResponseEntity<Map<String, Object>> handleCursor(CursorException ex) {
        HttpStatus status = (ex.reason() == CursorException.Reason.EXPIRED
                || ex.reason() == CursorException.Reason.COMPACTED)
                ? HttpStatus.GONE : HttpStatus.BAD_REQUEST;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "INVALID_CURSOR");
        body.put("reason", ex.reason().name());
        body.put("message", ex.getMessage() == null ? "" : ex.getMessage());
        if (ex.reason() == CursorException.Reason.COMPACTED) {
            body.put("resumeSeq", ex.resumeSeq());
            body.put("resumeCursor", ex.resumeCursor());
        }
        return ResponseEntity.status(status).body(body);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "BAD_REQUEST",
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
