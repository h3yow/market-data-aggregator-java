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
 *   <li>EXPIRED / UNAVAILABLE -> 410 Gone；UNAVAILABLE 额外携带
 *       recoverAfterSeq / recoverWindowStart / recoverCursor 三个恢复起点字段。</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CursorException.class)
    public ResponseEntity<Map<String, Object>> handleCursor(CursorException ex) {
        boolean gone = ex.reason() == CursorException.Reason.EXPIRED
                || ex.reason() == CursorException.Reason.UNAVAILABLE;
        HttpStatus status = gone ? HttpStatus.GONE : HttpStatus.BAD_REQUEST;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "INVALID_CURSOR");
        body.put("reason", ex.reason().name());
        body.put("message", ex.getMessage() == null ? "" : ex.getMessage());
        if (ex.reason() == CursorException.Reason.UNAVAILABLE) {
            body.put("recoverAfterSeq", ex.recoverAfterSeq());
            body.put("recoverWindowStart", ex.recoverWindowStart());
            body.put("recoverCursor", ex.recoverCursor());
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
