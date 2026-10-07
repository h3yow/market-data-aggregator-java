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
 *   <li>EXPIRED（超过 TTL）-> 410 Gone；</li>
 *   <li>COMPACTED（续拉位置已被压缩折叠）-> 410 Gone，并在 recovery 字段中给出
 *       继续向前起点（resumeFromSeq/resumeCursor，从压缩水位开始、seq 不重号；
 *       折叠区间窗口用区间 K 线补齐）与全量重建起点
 *       （replayFromSeq=0/fullReplayCursor，带恢复授权、可重放每个已结算窗口的结算事件）。</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CursorException.class)
    public ResponseEntity<Map<String, Object>> handleCursor(CursorException ex) {
        boolean gone = ex.reason() == CursorException.Reason.EXPIRED
                || ex.reason() == CursorException.Reason.COMPACTED;
        HttpStatus status = gone ? HttpStatus.GONE : HttpStatus.BAD_REQUEST;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "INVALID_CURSOR");
        body.put("reason", ex.reason().name());
        body.put("message", ex.getMessage() == null ? "" : ex.getMessage());
        if (ex.recovery() != null) {
            CursorException.Recovery r = ex.recovery();
            Map<String, Object> recovery = new LinkedHashMap<>();
            recovery.put("cursorSeq", r.cursorSeq());
            recovery.put("compactedThroughSeq", r.compactedThroughSeq());
            recovery.put("resumeFromSeq", r.resumeFromSeq());
            recovery.put("resumeCursor", r.resumeCursor());
            recovery.put("replayFromSeq", r.replayFromSeq());
            recovery.put("fullReplayCursor", r.fullReplayCursor());
            body.put("recovery", recovery);
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
