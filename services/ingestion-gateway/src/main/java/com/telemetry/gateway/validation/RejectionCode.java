package com.telemetry.gateway.validation;

/** Why one event of a batch was rejected. The codes are fixed by spec 8.1 and libs/telemetry-contracts/README.md. */
public enum RejectionCode {
    SCHEMA_VIOLATION,
    TIMESTAMP_OUT_OF_RANGE,
    SEMANTIC_VIOLATION,
    DUPLICATE_EVENT_ID,
    SERVICE_NOT_PERMITTED
}
