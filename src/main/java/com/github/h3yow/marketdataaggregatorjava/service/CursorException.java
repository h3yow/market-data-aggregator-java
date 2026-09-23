package com.github.h3yow.marketdataaggregatorjava.service;

/**
 * 游标非法或已过期时抛出，原因必须可区分。
 */
public class CursorException extends RuntimeException {

    public enum Reason {
        /** 游标不是服务端签发的格式 */
        MALFORMED,
        /** 签名校验失败（被篡改或来自其他实例） */
        BAD_SIGNATURE,
        /** 游标对应的事件已超出服务端保留范围 */
        EXPIRED,
        /** 游标属于其他标的 */
        SYMBOL_MISMATCH
    }

    private final Reason reason;

    public CursorException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
