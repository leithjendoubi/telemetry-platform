-- Agent registry of the ingestion context (spec 8.4, ingestion_db; FR-ING-05, FR-ING-06).
-- Only the SHA-256 of an API key is stored, never the key. Revoking an agent = status 'REVOKED' (403, spec 8.1).
CREATE TABLE agent (
    agent_id         text        PRIMARY KEY
                                 CHECK (agent_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    display_name     text        NOT NULL,
    api_key_hash     char(64)    NOT NULL UNIQUE          -- lower-case hex SHA-256; the unique index serves the lookup
                                 CHECK (api_key_hash ~ '^[0-9a-f]{64}$'),
    allowed_services text[]      NOT NULL
                                 CHECK (cardinality(allowed_services) > 0),
    rate_limit_rps   integer     NOT NULL DEFAULT 50  CHECK (rate_limit_rps > 0),   -- FR-ING-06 defaults
    burst            integer     NOT NULL DEFAULT 100 CHECK (burst > 0),
    status           text        NOT NULL DEFAULT 'ACTIVE'
                                 CHECK (status IN ('ACTIVE', 'REVOKED')),
    created_at       timestamptz NOT NULL DEFAULT now(),
    rotated_at       timestamptz
);

COMMENT ON TABLE agent IS 'Registered monitoring agents; read by the ingestion gateway (spec 8.4)';
