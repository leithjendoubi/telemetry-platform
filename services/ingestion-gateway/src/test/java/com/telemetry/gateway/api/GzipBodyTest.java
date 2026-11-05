package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

/** FR-ING-08 and spec 8.1: gzip bodies, with the 256 KiB limit applied after decompression. */
class GzipBodyTest {

    private static final int LIMIT = 262_144;

    static byte[] gzip(byte[] plain) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(plain);
        }
        return out.toByteArray();
    }

    @Test
    void aBatchComesOutAsItWentIn() throws Exception {
        byte[] json = "{\"schemaVersion\":\"1.0\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(GzipBody.unpack(gzip(json), LIMIT)).isEqualTo(json);
    }

    @Test
    void exactlyTheLimitIsFineOneByteMoreIsTooLarge() throws Exception {
        assertThat(GzipBody.unpack(gzip(new byte[LIMIT]), LIMIT)).hasSize(LIMIT);
        assertThatThrownBy(() -> GzipBody.unpack(gzip(new byte[LIMIT + 1]), LIMIT))
                .isInstanceOf(GzipBody.TooLarge.class);
    }

    @Test
    void aZipBombStopsAtTheLimit() throws Exception {
        byte[] bomb = gzip(new byte[10 * 1024 * 1024]); // 10 MiB of zeros
        assertThat(bomb.length).as("the bomb itself is small").isLessThan(LIMIT / 10);
        assertThatThrownBy(() -> GzipBody.unpack(bomb, LIMIT)).isInstanceOf(GzipBody.TooLarge.class);
    }

    @Test
    void brokenOrTruncatedGzipIsNotGzip() throws Exception {
        assertThatThrownBy(() -> GzipBody.unpack("{\"plain\":\"json\"}".getBytes(StandardCharsets.UTF_8), LIMIT))
                .isInstanceOf(GzipBody.NotGzip.class);
        byte[] whole = gzip("{\"schemaVersion\":\"1.0\"}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> GzipBody.unpack(Arrays.copyOf(whole, whole.length / 2), LIMIT))
                .isInstanceOf(GzipBody.NotGzip.class);
        assertThatThrownBy(() -> GzipBody.unpack(new byte[0], LIMIT)).isInstanceOf(GzipBody.NotGzip.class);
    }
}
