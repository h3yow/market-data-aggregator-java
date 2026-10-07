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
 * 常规游标 payload 为 {@code symbol|seq|issuedAtMillis}；压缩恢复游标多出第 4 段
 * 恢复前沿 {@code symbol|seq|issuedAtMillis|frontier}（seq 为压缩后日志首条保留
 * 事件之前的位置），客户端应把它当作不透明串透传。seq 为“本页之后的起点”，
 * 即已交给客户端的最大事件 seq（首次为 0）。
 *
 * <p>保证“重复拉取不重不漏”：服务端始终返回 {@code event.seq > cursor.seq}
 * 的事件，事件日志只追加、seq 单调，因此同一游标重放得到完全相同的结果。
 *
 * <p>压缩兼容判定（通过合法性/签名/TTL/标的校验之后）：
 * <ul>
 *   <li>请求不带游标（null/空）：始终从<b>当前压缩后日志</b>的开头拉取，
 *       不判失效——这就是压缩恢复路径；</li>
 *   <li>恢复游标（携带 frontier 标记）：只承诺最终视图一致，始终放行；
 *       若位置已落在“整段更老、不再保留任何事件”的区间，则在同一把锁内
 *       透明快进到当前日志开头后读取——重放期间继续压缩也不会被反复拒绝；</li>
 *   <li>常规游标 {@code seq >= compactedThroughSeq}：其后事件一条未删，逐条一致；</li>
 *   <li>常规游标 {@code seq < compactedThroughSeq}：续拉位置已被删除，
 *       以 {@link CursorException.Reason#COMPACTED} 明确拒绝并返回恢复游标。</li>
 * </ul>
 *
 * <p>拒绝原因互斥可区分：{@link CursorException.Reason#MALFORMED}（格式/字段错误）、
 * {@link CursorException.Reason#BAD_SIGNATURE}（签名不匹配/被篡改）、
 * {@link CursorException.Reason#EXPIRED}（超过 TTL）、
 * {@link CursorException.Reason#SYMBOL_MISMATCH}（游标属于其他标的）、
 * {@link CursorException.Reason#COMPACTED}（续拉位置已被压缩删除，无法逐条一致）。
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
     * 增量拉取。cursor 为 null 或空表示“从当前压缩后日志的开头开始”
     * （压缩恢复也走这里）。
     *
     * @throws CursorException 游标非法/过期/串标（{@link #decode}），
     *                         或续拉位置已被压缩删除（{@link CursorException.Reason#COMPACTED}，
     *                         携带可恢复起点与新游标）
     */
    public CursorPage pull(String symbol, String cursor, int requestedLimit) {
        int limit = Math.max(MIN_LIMIT, Math.min(MAX_LIMIT, requestedLimit));

        // 首次拉取 / 压缩后恢复：无游标即从当前压缩后日志开头开始，永不判失效，
        // 与读取在同一把标的锁内完成。尚未压缩过（frontier=0）时签发常规游标；
        // 一旦压缩过，从头拉取天然走“恢复游标”（只承诺最终视图一致）。
        if (cursor == null || cursor.isBlank()) {
            AggregationStore.PullOutcome first = store.pullFrom(symbol, 0L, true, limit);
            return readPage(symbol, first, null, limit, first.compactedThroughSeq() > 0);
        }

        ParsedCursor parsed = decode(symbol, cursor);
        boolean recovery = parsed.recoveryFrontier >= 0;
        // 判定 + 读取在 AggregationStore 的同一 synchronized(state) 临界区内，
        // 压缩不可能插在中间。恢复游标始终放行（只承诺最终视图）；
        // 常规游标若其后有被删事件（seq < 前沿），则明确拒绝并给恢复起点。
        AggregationStore.PullOutcome outcome =
                store.pullFrom(symbol, parsed.seq, recovery, limit);
        if (outcome.rejected()) {
            long resumeSeq = outcome.resumeHead();
            String resumeCursor = issueResume(symbol, resumeSeq, outcome.compactedThroughSeq());
            log.info("[CURSOR] symbol={} 续拉 seq={} 已被压缩（frontier={}），拒绝并返回恢复起点 seq={}",
                    symbol, parsed.seq, outcome.compactedThroughSeq(), resumeSeq);
            throw new CursorException(CursorException.Reason.COMPACTED,
                    "游标续拉位置 seq=" + parsed.seq + " 所在的事件已被压缩删除（压缩前沿 seq="
                            + outcome.compactedThroughSeq() + "），无法再保证与压缩前逐条一致；"
                            + "请丢弃本地增量状态，改用返回的新游标（resumeSeq=" + resumeSeq
                            + "，即压缩后日志开头）重放以恢复全部窗口最终状态",
                    resumeSeq, resumeCursor);
        }
        return readPage(symbol, outcome, cursor, limit, recovery);
    }

    private CursorPage readPage(String symbol, AggregationStore.PullOutcome outcome,
                                 String originalCursor, int limit, boolean recovery) {
        List<CursorEvent> events = outcome.events();
        long lastSeq = events.isEmpty() ? outcome.afterSeq() : events.get(events.size() - 1).seq();
        boolean hasMore = lastSeq < store.maxSeq(symbol);
        // 本页为空（位置未推进）时回传原游标，保证同一位置反复拉取得到相同游标字符串。
        String nextCursor;
        if (events.isEmpty() && originalCursor != null) {
            nextCursor = originalCursor;
        } else if (recovery) {
            // 恢复路径上的下一页继续保持“恢复游标”身份（携带当前前沿），
            // 不能退化成常规游标，否则下一页会被误判逐条一致而拒绝。
            nextCursor = issueResume(symbol, lastSeq, outcome.compactedThroughSeq());
        } else {
            nextCursor = issue(symbol, lastSeq);
        }
        log.info("[CURSOR] symbol={} afterSeq={} recovery={} limit={} 返回 {} 条, lastSeq={}, hasMore={}",
                symbol, outcome.afterSeq(), recovery, limit, events.size(), lastSeq, hasMore);
        return new CursorPage(symbol, events, nextCursor, hasMore);
    }

    /**
     * 签发常规游标（3 段 payload：symbol|seq|issuedAt）。
     */
    public String issue(String symbol, long seq) {
        return encode(symbol + "|" + seq + "|" + System.currentTimeMillis());
    }

    /**
     * 签发压缩恢复游标（4 段 payload：symbol|seq|issuedAt|recoveryFrontier）。
     * seq 指向压缩后日志开头（{@code firstRetainedSeq - 1}），
     * recoveryFrontier 标记它是针对哪个压缩前沿恢复，用于与常规游标区分。
     */
    private String issueResume(String symbol, long seq, long recoveryFrontier) {
        return encode(symbol + "|" + seq + "|" + System.currentTimeMillis() + "|" + recoveryFrontier);
    }

    private String encode(String payload) {
        String payloadPart = encoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return payloadPart + "." + encoder.encodeToString(hmac(payloadPart));
    }

    /** 解析结果。{@code recoveryFrontier} 为 -1 表示常规游标，否则为压缩恢复游标。 */
    private record ParsedCursor(String symbol, long seq, long issuedAt, long recoveryFrontier) {
    }

    /**
     * 校验游标并返回其携带内容。任何非法情况都以明确原因抛出 {@link CursorException}。
     */
    private ParsedCursor decode(String expectedSymbol, String cursor) {
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
                    "游标载荷字段数量错误，应为 symbol|seq|issuedAt[|frontier]");
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
        long recoveryFrontier = -1L;
        if (parts.length == 4) {
            try {
                recoveryFrontier = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                throw new CursorException(CursorException.Reason.MALFORMED,
                        "恢复游标中的 frontier 不是整数");
            }
            if (recoveryFrontier < 0) {
                throw new CursorException(CursorException.Reason.MALFORMED,
                        "恢复游标 frontier 不能为负");
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
                            + "ms），请丢弃后不带 cursor 重新从头（压缩后日志开头）拉取");
        }
        return new ParsedCursor(cursorSymbol, seq, issuedAt, recoveryFrontier);
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
