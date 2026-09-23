package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 游标编解码。
 *
 * <p>明文载荷为 {@code symbol|seq|issuedAt}（base64url 编码），
 * 后附 HMAC-SHA256 签名，保证游标不可伪造、不可跨标的复用；
 * 解码时同时校验签名、TTL 与标的，任何不符都抛出 {@link InvalidCursorException}。
 */
@Component
public class CursorCodec {

    private static final String HMAC_ALGO = "HmacSHA256";

    private final AggregatorProperties properties;
    private final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    private final Base64.Decoder decoder = Base64.getUrlDecoder();

    public CursorCodec(AggregatorProperties properties) {
        this.properties = properties;
    }

    public static final class Cursor {
        private final String symbol;
        private final long seq;
        private final long issuedAt;

        public Cursor(String symbol, long seq, long issuedAt) {
            this.symbol = symbol;
            this.seq = seq;
            this.issuedAt = issuedAt;
        }

        public String getSymbol() { return symbol; }
        public long getSeq() { return seq; }
        public long getIssuedAt() { return issuedAt; }
    }

    public String encode(String symbol, long seq, long now) {
        String payload = encodePayload(symbol, seq, now);
        return payload + "." + sign(payload);
    }

    /**
     * 解码并校验游标。
     *
     * @param expectedSymbol 该接口路径对应的标的，游标跨标的使用会被拒绝
     */
    public Cursor decode(String token, String expectedSymbol, long now) {
        if (token == null || token.isBlank()) {
            throw new InvalidCursorException("cursor is missing");
        }
        int dot = token.lastIndexOf('.');
        if (dot <= 0 || dot == token.length() - 1) {
            throw new InvalidCursorException("cursor is malformed");
        }
        String payload = token.substring(0, dot);
        String signature = token.substring(dot + 1);
        if (!constantTimeEquals(signature, sign(payload))) {
            throw new InvalidCursorException("cursor signature does not match");
        }
        String plain;
        try {
            plain = new String(decoder.decode(payload), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            throw new InvalidCursorException("cursor payload is malformed");
        }
        String[] parts = plain.split("\\|", -1);
        if (parts.length != 3) {
            throw new InvalidCursorException("cursor payload is malformed");
        }
        long seq;
        long issuedAt;
        try {
            seq = Long.parseLong(parts[1]);
            issuedAt = Long.parseLong(parts[2]);
        } catch (NumberFormatException ex) {
            throw new InvalidCursorException("cursor payload is malformed");
        }
        if (seq < 0) {
            throw new InvalidCursorException("cursor sequence is negative");
        }
        if (!parts[0].equals(expectedSymbol)) {
            throw new InvalidCursorException("cursor belongs to a different symbol");
        }
        long ttl = properties.getCursorTtlMs();
        if (issuedAt > now) {
            throw new InvalidCursorException("cursor issued-at is in the future");
        }
        if (now - issuedAt > ttl) {
            throw new InvalidCursorException("cursor has expired");
        }
        return new Cursor(parts[0], seq, issuedAt);
    }

    private String encodePayload(String symbol, long seq, long now) {
        String plain = symbol + "|" + seq + "|" + now;
        return encoder.encodeToString(plain.getBytes(StandardCharsets.UTF_8));
    }

    private String sign(String payload) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(new SecretKeySpec(
                    properties.getCursorSecret().getBytes(StandardCharsets.UTF_8), HMAC_ALGO));
            return encoder.encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception ex) {
            throw new IllegalStateException("failed to sign cursor", ex);
        }
    }

    private boolean constantTimeEquals(String a, String b) {
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < x.length; i++) {
            diff |= x[i] ^ y[i];
        }
        return diff == 0;
    }
}
