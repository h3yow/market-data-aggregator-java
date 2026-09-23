package com.github.h3yow.marketdataaggregatorjava.tests;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.service.CursorCodec;
import com.github.h3yow.marketdataaggregatorjava.service.InvalidCursorException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CursorCodecTest {

    private CursorCodec codec;
    private AggregatorProperties props;

    @BeforeEach
    void setUp() {
        props = new AggregatorProperties();
        props.setCursorTtlMs(1000);
        props.setCursorSecret("unit-test-secret");
        codec = new CursorCodec(props);
    }

    @Test
    void roundTrip() {
        String token = codec.encode("AAA", 42L, 10_000L);
        CursorCodec.Cursor c = codec.decode(token, "AAA", 10_500L);
        assertEquals("AAA", c.getSymbol());
        assertEquals(42L, c.getSeq());
        assertEquals(10_000L, c.getIssuedAt());
    }

    @Test
    void rejectsTamperedPayload() {
        String token = codec.encode("AAA", 42L, 10_000L);
        char flipped = token.charAt(0) == 'A' ? 'B' : 'A';
        String tampered = flipped + token.substring(1);
        InvalidCursorException ex = assertThrows(InvalidCursorException.class,
                () -> codec.decode(tampered, "AAA", 10_500L));
        org.assertj.core.api.Assertions.assertThat(ex.getMessage()).contains("signature");
    }

    @Test
    void rejectsCrossSymbol() {
        String token = codec.encode("AAA", 42L, 10_000L);
        InvalidCursorException ex = assertThrows(InvalidCursorException.class,
                () -> codec.decode(token, "BBB", 10_500L));
        org.assertj.core.api.Assertions.assertThat(ex.getMessage()).contains("different symbol");
    }

    @Test
    void rejectsExpired() {
        String token = codec.encode("AAA", 42L, 10_000L);
        InvalidCursorException ex = assertThrows(InvalidCursorException.class,
                () -> codec.decode(token, "AAA", 12_001L));
        org.assertj.core.api.Assertions.assertThat(ex.getMessage()).contains("expired");
    }

    @Test
    void rejectsGarbage() {
        assertThrows(InvalidCursorException.class,
                () -> codec.decode("not-a-cursor", "AAA", 10_000L));
        assertThrows(InvalidCursorException.class,
                () -> codec.decode("", "AAA", 10_000L));
    }
}
