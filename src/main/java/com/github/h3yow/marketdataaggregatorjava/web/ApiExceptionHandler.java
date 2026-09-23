package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.service.InvalidCursorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 统一错误响应：非法/过期游标返回 400，错误码可区分。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidCursorException.class)
    public ResponseEntity<Map<String, String>> handleInvalidCursor(InvalidCursorException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "INVALID_CURSOR", "message", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "INVALID_ARGUMENT", "message", ex.getMessage()));
    }
}
