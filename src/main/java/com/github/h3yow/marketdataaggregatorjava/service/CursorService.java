package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * 游标签发、校验与增量拉取。
 *
 * <p>游标为不透明字符串：{@code base64url(payload) + "." + base64url(hmac)}。
 * 普通游标 payload 为 {@code symbol|seq|issuedAtMillis}；压缩恢复游标多出第 4 段
 * {@code R<recoveryUpTo>}（如 {@code symbol|0|issuedAt|R139}），由服务端在压缩拒绝时
 * 签发，表示“客户端已被告知发生过截至水位 recoveryUpTo 的折叠”。它不按 seq 区间授权
 * （折叠前保留的结算事件 seq 可能晚于水位），而是在当前水位未超过 recoveryUpTo 时
 * 允许整个重放过程越过压缩水位门，追平日志末尾后自动回归普通游标。该段受同一 HMAC
 * 保护，客户端无法伪造。seq 为“已交给客户端的最大事件 seq”（首次为 0）。
 *
 * <p>保证“重复拉取不重不漏”：正常情况下服务端始终返回 {@code event.seq > cursor.seq}
 * 的事件，事件日志只追加、seq 单调。
 *
 * <p>拒绝原因互斥可区分：{@link CursorException.Reason#MALFORMED}（格式/字段错误）、
 * {@link CursorException.Reason#BAD_SIGNATURE}（签名不匹配/被篡改）、
 * {@link CursorException.Reason#EXPIRED}（超过 TTL）、
 * {@link CursorException.Reason#SYMBOL_MISMATCH}（游标属于其他标的）、
 * {@link CursorException.Reason#COMPACTED}（游标续拉位置之后的事件已被压缩折叠，
 * 无法逐条一致；异常携带恢复起点与带授权的新游标）。
 */
@Service
public class CursorService {

    private static final Logger log = LoggerFactory.getLogger(CursorService.class);
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int MIN_LIMIT = 1;
    private static final int MAX_LIMIT = 1000;
    private static final String RECOVERY_PREFIX = "R";

    private final AggregationStore store;
    private final MarketDataProperties properties;
    private final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    private final Base64.Decoder decoder = Base64.getUrlDecoder();

    /** 解析后的游标位置；recoveryUpTo=0 表示普通游标。 */
    private record ParsedCursor(long afterSeq, long recoveryUpTo) {
    }

    public CursorService(AggregationStore store, MarketDataProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * 增量拉取。cursor 为 null 或空表示从头开始（seq=0）。
     *
     * <p>游标校验通过后，压缩水位判定与事件读取在存储层同一把锁内原子完成。
     * 若游标续拉位置之后的事件已被压缩折叠，抛出 {@link CursorException}
     * （{@link CursorException.Reason#COMPACTED}），并给出两种可恢复起点：
     * 继续向前（从压缩水位开始，seq 不重号，但折叠区间窗口需用 K 线补齐）或
     * 全量重建（带恢复授权的从头游标，可重放每个已结算窗口保留下来的结算事件）。
     */
    public CursorPage pull(String symbol, String cursor, int requestedLimit) {
        int limit = Math.max(MIN_LIMIT, Math.min(MAX_LIMIT, requestedLimit));
        ParsedCursor parsed = decode(symbol, cursor);
        long afterSeq = parsed.afterSeq();
        long recoveryUpTo = parsed.recoveryUpTo();

        AggregationStore.PullResult result = store.pullEvents(symbol, afterSeq, recoveryUpTo, limit);
        if (!result.ok()) {
            AggregationStore.CompactedRecovery r = result.compacted();
            throw compactedException(symbol, r);
        }

        List<CursorEvent> events = result.events();
        long lastSeq = events.isEmpty() ? afterSeq : events.get(events.size() - 1).seq();
        boolean hasMore = lastSeq < store.maxSeq(symbol);
        String nextCursor;
        if (events.isEmpty() && cursor != null && !cursor.isBlank()) {
            // 位置未推进：回传原游标，保证同一位置反复拉取结果与游标字符串稳定。
            nextCursor = cursor;
        } else if (recoveryUpTo > 0 && lastSeq < store.maxSeq(symbol)) {
            // 仍在全量重建途中（还没读到当前日志末尾）：折叠前保留下来的结算事件 seq
            // 可能晚于压缩水位，续页必须继续携带恢复授权才能越过水位门。
            // 追平 maxSeq 之后只剩正常尾部追加，转为普通游标，授权不无限延续。
            nextCursor = issueRecovery(symbol, lastSeq, recoveryUpTo);
        } else {
            nextCursor = issue(symbol, lastSeq);
        }
        log.info("[CURSOR] symbol={} afterSeq={} recoveryUpTo={} limit={} 返回 {} 条, lastSeq={}, hasMore={}",
                symbol, afterSeq, recoveryUpTo, limit, events.size(), lastSeq, hasMore);
        return new CursorPage(symbol, List.copyOf(events), nextCursor, hasMore);
    }

    private CursorException compactedException(String symbol, AggregationStore.CompactedRecovery r) {
        // 恢复起点①：从压缩水位 F 继续向前（其后事件全部保留，seq 只向前、绝不重号）。
        // 折叠区间 (c, F] 内的窗口若客户端没有把握，改用区间 K 线查询补齐最终状态。
        long frontier = r.compactedThroughSeq();
        String resumeCursor = issue(symbol, frontier);
        // 恢复起点②：带恢复授权的从头游标，重放日志中保留下来的全部事件
        // （每个已结算窗口都保留其 WINDOW_SETTLED 最终快照），丢弃本地状态后可全量重建。
        long replayFromSeq = 0L;
        String fullReplayCursor = issueRecovery(symbol, replayFromSeq, frontier);
        String message = "游标（seq=" + r.cursorSeq() + "）续拉位置之后的事件（seq 至 "
                + frontier + "）已被压缩折叠，无法保证与压缩前逐条一致，故拒绝静默续拉。"
                + "可二选一恢复：① 用 resumeCursor(seq=" + frontier
                + ") 从压缩水位继续向前（新事件 seq 不重号；seq 在 (" + r.cursorSeq() + ", "
                + frontier + "] 的已结算窗口请改用区间 K 线查询补齐最终状态）；"
                + "② 丢弃本地状态后用 fullReplayCursor(seq=0, 授权至 " + frontier
                + ") 从头全量重建，得到的窗口最终状态与区间 K 线完全一致。";
        log.info("[CURSOR] symbol={} afterSeq={} 被压缩拒绝，compactedThrough={}, earliest={}",
                symbol, r.cursorSeq(), frontier, r.earliestRetainedSeq());
        return new CursorException(CursorException.Reason.COMPACTED, message,
                new CursorException.Recovery(r.cursorSeq(), frontier,
                        frontier, resumeCursor, replayFromSeq, fullReplayCursor));
    }

    /**
     * 签发普通游标。
     */
    public String issue(String symbol, long seq) {
        return encode(symbol + "|" + seq + "|" + System.currentTimeMillis());
    }

    /**
     * 签发压缩恢复游标：在普通载荷后追加受 HMAC 保护的授权段 {@code R<recoveryUpTo>}。
     */
    private String issueRecovery(String symbol, long seq, long recoveryUpTo) {
        return encode(symbol + "|" + seq + "|" + System.currentTimeMillis()
                + "|" + RECOVERY_PREFIX + recoveryUpTo);
    }

    private String encode(String payload) {
        String payloadPart = encoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String sigPart = encoder.encodeToString(hmac(payloadPart));
        return payloadPart + "." + sigPart;
    }

    /**
     * 校验游标并返回其携带的位置（seq 与恢复授权）。任何非法情况都以明确原因抛出
     * {@link CursorException}。
     */
    private ParsedCursor decode(String expectedSymbol, String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return new ParsedCursor(0L, 0L);
        }
        int dot = cursor.indexOf('.');
        if (dot <= 0 || dot == cursor.length() - 1 || cursor.indexOf('.', dot + 1) >= 0) {
            throw new CursorException(CursorException.Reason.MALFORMED,
                    "游标格式错误：应为 payload.signature 两段式");
        }
        String payloadPart = cursor.substring(0, dot);
        String sigPart = cursor.substring(dot + 1);

        byte[] expectedSig;
        byte[] payloadBytes;
        try {
            expectedSig = decoder.decode(sigPart);
            payloadBytes = decoder.decode(payloadPart);
        } catch (IllegalArgumentException e) {
            throw new CursorException(CursorException.Reason.MALFORMED,
                    "游标不是合法的 base64url 编码");
        }

        byte[] actualSig = hmac(payloadPart);
        if (!constantTimeEquals(expectedSig, actualSig)) {
            throw new CursorException(CursorException.Reason.BAD_SIGNATURE,
                    "游标签名校验失败：可能被篡改或来自其他实例");
        }

        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3 && parts.length != 4) {
            throw new CursorException(CursorException.Reason.MALFORMED,
                    "游标载荷字段数量错误，应为 symbol|seq|issuedAt[|RrecoveryUpTo]");
        }
        String cursorSymbol = parts[0];
        long seq;
        long issuedAt;
        try {
            seq = Long.parseLong(parts[1]);
            issuedAt = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            throw new CursorException(CursorException.Reason.MALFORMED,
                    "游标中的 seq/issuedAt 不是整数");
        }
        if (seq < 0) {
            throw new CursorException(CursorException.Reason.MALFORMED, "游标 seq 不能为负");
        }
        long recoveryUpTo = 0L;
        if (parts.length == 4) {
            String tag = parts[3];
            if (!tag.startsWith(RECOVERY_PREFIX) || tag.length() <= RECOVERY_PREFIX.length()) {
                throw new CursorException(CursorException.Reason.MALFORMED,
                        "游标恢复授权段格式错误，应为 R<recoveryUpTo>");
            }
            try {
                recoveryUpTo = Long.parseLong(tag.substring(RECOVERY_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new CursorException(CursorException.Reason.MALFORMED,
                        "游标恢复授权 recoveryUpTo 不是整数");
            }
            if (recoveryUpTo <= 0) {
                throw new CursorException(CursorException.Reason.MALFORMED,
                        "恢复游标必须携带正的 recoveryUpTo");
            }
        }

        if (!cursorSymbol.equals(expectedSymbol)) {
            throw new CursorException(CursorException.Reason.SYMBOL_MISMATCH,
                    "游标属于标的 " + cursorSymbol + "，不能用于拉取 " + expectedSymbol);
        }
        long age = System.currentTimeMillis() - issuedAt;
        if (age > properties.getCursorTtlMillis()) {
            throw new CursorException(CursorException.Reason.EXPIRED,
                    "游标已过期（签发于 " + age + "ms 前，TTL=" + properties.getCursorTtlMillis()
                            + "ms），请丢弃后从头或用新游标拉取");
        }
        return new ParsedCursor(seq, recoveryUpTo);
    }

    private byte[] hmac(String payloadPart) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            byte[] keyBytes = properties.getCursorSecret().getBytes(StandardCharsets.UTF_8);
            mac.init(new SecretKeySpec(keyBytes, HMAC_ALGORITHM));
            return mac.doFinal(payloadPart.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("HMAC 初始化失败", e);
        }
    }

    private boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }
}
