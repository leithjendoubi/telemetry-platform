package com.telemetry.gateway.ratelimit;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global cap on requests in progress (FR-ING-06 second half, NFR-REL-04; spec 6.8: 2,000 per gateway instance). Above
 * it the gateway answers 503 with Retry-After instead of queueing: overload is signalled, never buffered without
 * limit (spec principle 6, "backpressure over buffering").
 */
public class InFlightLimiter {

    private final int max;
    private final AtomicInteger inFlight = new AtomicInteger();

    public InFlightLimiter(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("max in-flight must be positive");
        }
        this.max = max;
    }

    /** Takes a slot; false when all {@code max} are taken. Every true must be followed by one {@link #release}. */
    public boolean tryAcquire() {
        while (true) {
            int current = inFlight.get();
            if (current >= max) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    public void release() {
        inFlight.decrementAndGet();
    }

    /** Requests in progress now (for metrics, step B1). */
    public int inFlight() {
        return inFlight.get();
    }
}
