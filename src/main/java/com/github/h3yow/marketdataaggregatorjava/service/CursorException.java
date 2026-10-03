package com.github.h3yow.marketdataaggregatorjava.service;

/**
 * 游标非法、已过期或已不可用时抛出，原因必须可区分。
 */
public class CursorException extends RuntimeException {

    public enum Reason {
        /** 游标不是服务端签发的格式 */
        MALFORMED,
        /** 签名校验失败（被篡改或来自其他实例） */
        BAD_SIGNATURE,
        /** 游标超过 TTL（410 语义） */
        EXPIRED,
        /** 游标属于其他标的 */
        SYMBOL_MISMATCH,
        /** 游标指向的事件已被压缩且归档被物理清除，无法再从该位置重放（410 语义） */
        UNAVAILABLE
    }

    private final Reason reason;

    /** 仅 UNAVAILABLE 使用：可恢复重放的最早 seq（从该 seq 之后继续）；其他原因为 null */
    private final Long recoverAfterSeq;

    /** 仅 UNAVAILABLE 使用：可恢复起点对应的最早窗口起点（毫秒）；其他原因为 null */
    private final Long recoverWindowStart;

    /** 仅 UNAVAILABLE 使用：可恢复起点的新游标；其他原因为 null */
    private String recoverCursor;

    public CursorException(Reason reason, String message) {
        this(reason, message, null, null);
    }

    public CursorException(Reason reason, String message,
                           Long recoverAfterSeq, Long recoverWindowStart) {
        super(message);
        this.reason = reason;
        this.recoverAfterSeq = recoverAfterSeq;
        this.recoverWindowStart = recoverWindowStart;
    }

    public Reason reason() {
        return reason;
    }

    public Long recoverAfterSeq() {
        return recoverAfterSeq;
    }

    public Long recoverWindowStart() {
        return recoverWindowStart;
    }

    public String recoverCursor() {
        return recoverCursor;
    }

    public CursorException withRecoverCursor(String cursor) {
        this.recoverCursor = cursor;
        return this;
    }
}
