package com.github.h3yow.marketdataaggregatorjava.service;

/**
 * 游标非法或过期。
 */
public class InvalidCursorException extends RuntimeException {
    public InvalidCursorException(String message) {
        super(message);
    }
}
