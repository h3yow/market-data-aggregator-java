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
        /** 游标超过 TTL，已超出服务端承诺的有效期 */
        EXPIRED,
        /** 游标属于其他标的 */
        SYMBOL_MISMATCH,
        /**
         * 游标本身合法且未过期，但其续拉位置之后的事件已被日志压缩折叠，
         * 无法再保证“与未压缩时逐条一致”，故明确拒绝静默续拉，
         * 并随异常给出可恢复起点与新游标。
         */
        COMPACTED
    }

    /** 压缩拒绝时的恢复指引；其余原因为 null。 */
    public record Recovery(long cursorSeq,
                           long compactedThroughSeq,
                           long resumeFromSeq,
                           String resumeCursor,
                           long replayFromSeq,
                           String fullReplayCursor) {
        // cursorSeq           ：被拒游标携带的 seq
        // compactedThroughSeq ：当前压缩水位 F（被折叠事件的最大 seq）
        // resumeFromSeq       ：继续向前的恢复起点 = F。从该位置续拉 seq 只向前、不重号；
        //                        但游标与恢复起点之间 (cursorSeq, F] 已被折叠，
        //                        若客户端对这些窗口的状态没有把握，应改用全量重建，
        //                        或用区间 K 线查询补齐已结算窗口的最终状态。
        // resumeCursor        ：签在 resumeFromSeq 的新游标。
        // replayFromSeq       ：全量重建起点，恒为 0。
        // fullReplayCursor    ：从头游标；丢弃本地状态后重放，可重建全部窗口最终状态。
    }

    private final Reason reason;
    private final Recovery recovery;

    public CursorException(Reason reason, String message) {
        this(reason, message, null);
    }

    public CursorException(Reason reason, String message, Recovery recovery) {
        super(message);
        this.reason = reason;
        this.recovery = recovery;
    }

    public Reason reason() {
        return reason;
    }

    public Recovery recovery() {
        return recovery;
    }
}
