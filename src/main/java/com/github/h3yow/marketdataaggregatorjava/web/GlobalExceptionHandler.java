package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.service.CursorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 统一异常处理。
 *
 * <p>非法游标按 {@link CursorException.Reason} 返回可区分错误码：
 * <ul>
 *   <li>MALFORMED / BAD_SIGNATURE / SYMBOL_MISMATCH -> 400；</li>
 *   <li>EXPIRED -> 410 Gone。</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CursorException.class)
    public ResponseEntity<Map<String, String>> handleCursor(CursorException ex) {
        HttpStatus status = ex.reason() == CursorException.Reason.EXPIRED
                ? HttpStatus.GONE : HttpStatus.BAD_REQUEST;
        return ResponseEntity.status(status).body(Map.of(
                "error", "INVALID_CURSOR",
                "reason", ex.reason().name(),
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "BAD_REQUEST",
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
