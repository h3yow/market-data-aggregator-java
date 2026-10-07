package com.github.h3yow.marketdataaggregatorjava.service;

/**
 * 游标非法、已过期或续拉位置已被压缩时抛出，原因必须可区分。
 */
public class CursorException extends RuntimeException {

    public enum Reason {
        /** 游标不是服务端签发的格式 */
        MALFORMED,
        /** 签名校验失败（被篡改或来自其他实例） */
        BAD_SIGNATURE,
        /** 游标对应的事件已超出服务端保留范围（超过 TTL） */
        EXPIRED,
        /** 游标属于其他标的 */
        SYMBOL_MISMATCH,
        /**
         * 游标续拉位置所在的事件已被压缩删除，无法再保证“逐条一致”。
         * 与正常拉取明确区分：拒绝本次续拉，并携带可恢复起点 {@link #resumeSeq()}
         * 与新游标 {@link #resumeCursor()}，客户端清掉本地增量状态后从该起点重放，
         * 仍能重建出与区间查询一致的最终窗口状态。
         */
        COMPACTED
    }

    private final Reason reason;
    /** 可恢复起点 seq（COMPACTED 时有效）：从该 seq（不含）开始拉取可无损重建最终视图。 */
    private final long resumeSeq;
    /** 可恢复起点对应的新签发游标（COMPACTED 时有效，非 COMPACTED 为 null）。 */
    private final String resumeCursor;

    public CursorException(Reason reason, String message) {
        this(reason, message, -1L, null);
    }

    public CursorException(Reason reason, String message, long resumeSeq, String resumeCursor) {
        super(message);
        this.reason = reason;
        this.resumeSeq = resumeSeq;
        this.resumeCursor = resumeCursor;
    }

    public Reason reason() {
        return reason;
    }

    public long resumeSeq() {
        return resumeSeq;
    }

    public String resumeCursor() {
        return resumeCursor;
    }
}
