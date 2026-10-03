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
 * <p>游标为不透明字符串：{@code base64url(payload) + "." + base64url(hmac)}，
 * payload 为 {@code symbol|seq|issuedAtMillis}。seq 为“本页之后的起点”，
 * 即已交给客户端的最大事件 seq（首次为 0）。
 *
 * <p>保证“重复拉取不重不漏”：服务端始终返回 {@code event.seq > cursor.seq}
 * 的事件，事件日志只追加、seq 单调，因此同一游标重放得到完全相同的结果。
 *
 * <p>拒绝原因互斥可区分：{@link CursorException.Reason#MALFORMED}（格式/字段错误）、
 * {@link CursorException.Reason#BAD_SIGNATURE}（签名不匹配/被篡改）、
 * {@link CursorException.Reason#EXPIRED}（超过 TTL）、
 * {@link CursorException.Reason#SYMBOL_MISMATCH}（游标属于其他标的）。
 */
@Service
public class CursorService {

    private static final Logger log = LoggerFactory.getLogger(CursorService.class);
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int MIN_LIMIT = 1;
    private static final int MAX_LIMIT = 1000;

    private final AggregationStore store;
    private final MarketDataProperties properties;
    private final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    private final Base64.Decoder decoder = Base64.getUrlDecoder();

    public CursorService(AggregationStore store, MarketDataProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * 增量拉取。cursor 为 null 或空表示从头开始（seq=0）。
     */
    public CursorPage pull(String symbol, String cursor, int requestedLimit) {
        int limit = Math.max(MIN_LIMIT, Math.min(MAX_LIMIT, requestedLimit));
        long afterSeq = decode(symbol, cursor);
        checkAvailable(symbol, afterSeq);

        List<CursorEvent> events = store.readEvents(symbol, afterSeq, limit);
        long lastSeq = events.isEmpty() ? afterSeq : events.get(events.size() - 1).seq();
        boolean hasMore = lastSeq < store.maxSeq(symbol);
        // 游标由 (symbol, seq) 定位；本页为空（位置未推进）时回传原游标，
        // 保证同一位置反复拉取得到相同游标字符串，重放稳定。
        String nextCursor = events.isEmpty() && cursor != null && !cursor.isBlank()
                ? cursor : issue(symbol, lastSeq);
        log.info("[CURSOR] symbol={} afterSeq={} limit={} 返回 {} 条, lastSeq={}, hasMore={}",
                symbol, afterSeq, limit, events.size(), lastSeq, hasMore);
        return new CursorPage(symbol, List.copyOf(events), nextCursor, hasMore);
    }

    /**
     * 判断游标位置是否仍在服务端可回放范围内。
     *
     * <p>仅当已结算历史被“归档后物理清除”时，早于热日志起点的位置才无法服务：
     * 归档保留期间，旧游标始终经归档段完整回放。无法服务时抛出
     * {@link CursorException.Reason#UNAVAILABLE}，并给出可恢复起点
     * （recoverAfterSeq + 对应窗口起点）与新游标。
     */
    private void checkAvailable(String symbol, long afterSeq) {
        long hotBase = store.trimmedBaseSeq(symbol);
        if (hotBase <= 0 || afterSeq >= hotBase) {
            return;
        }
        long[] point = store.recoveryPoint(symbol);
        Long recoverAfterSeq = point == null ? null : point[0];
        Long recoverWindowStart = point == null ? null : point[1];
        String recoverCursor = point == null ? null : issue(symbol, point[0]);
        throw new CursorException(CursorException.Reason.UNAVAILABLE,
                "游标位置 seq=" + afterSeq + " 对应的事件已被压缩并物理清除（不可用边界 seq<"
                        + hotBase + "），无法从该位置继续重放；请从可恢复起点"
                        + " recoverAfterSeq=" + recoverAfterSeq
                        + "（windowStart=" + recoverWindowStart + "）使用 recoverCursor 继续",
                recoverAfterSeq, recoverWindowStart)
                .withRecoverCursor(recoverCursor);
    }

    /**
     * 为恢复起点签发票据（供异常处理器向客户端下发 recoverCursor）。
     */
    public String recoverCursor(String symbol, Long recoverAfterSeq) {
        return recoverAfterSeq == null ? null : issue(symbol, recoverAfterSeq);
    }

    /**
     * 签发游标。
     */
    public String issue(String symbol, long seq) {
        long issuedAt = System.currentTimeMillis();
        String payload = symbol + "|" + seq + "|" + issuedAt;
        String payloadPart = encoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String sigPart = encoder.encodeToString(hmac(payloadPart));
        return payloadPart + "." + sigPart;
    }

    /**
     * 校验游标并返回其携带的 seq。任何非法情况都以明确原因抛出 {@link CursorException}。
     */
    public long decode(String expectedSymbol, String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0L;
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
        if (parts.length != 3) {
            throw new CursorException(CursorException.Reason.MALFORMED,
                    "游标载荷字段数量错误，应为 symbol|seq|issuedAt");
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
        return seq;
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
