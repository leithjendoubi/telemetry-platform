package com.telemetry.gateway.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Unpacks a gzip request body (FR-ING-08). Spec 8.1: the 256 KiB limit applies after decompression, so unpacking
 * stops as soon as the output passes the limit: a few KiB of "zip bomb" cannot make the gateway allocate gigabytes.
 * Works on bytes already in memory, so it never blocks the event loop on I/O (FR-ING-02).
 */
final class GzipBody {

    /** The body unpacks to more than the limit: 413. */
    static final class TooLarge extends Exception {
        TooLarge() {
            super(null, null, false, false);
        }
    }

    /** The body is not valid gzip: 400. */
    static final class NotGzip extends Exception {
        NotGzip(String message) {
            super(message, null, false, false);
        }
    }

    private GzipBody() {}

    static byte[] unpack(byte[] compressed, int maxBytes) throws TooLarge, NotGzip {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(maxBytes, compressed.length * 4 + 64));
            byte[] chunk = new byte[8192];
            int total = 0;
            int n;
            while ((n = in.read(chunk)) != -1) {
                total += n;
                if (total > maxBytes) {
                    throw new TooLarge();
                }
                out.write(chunk, 0, n);
            }
            return out.toByteArray();
        } catch (ZipException e) {
            throw new NotGzip("Content-Encoding is gzip, but the body is not valid gzip: " + e.getMessage());
        } catch (IOException e) {
            // e.g. a truncated stream ("Unexpected end of ZLIB input stream")
            throw new NotGzip("Content-Encoding is gzip, but the body is not valid gzip: " + e.getMessage());
        }
    }
}
