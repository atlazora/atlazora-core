-- W00-WU05 — Event & Outbox Foundation
--
-- This schema is transport-independent.
-- It does not select a broker, routing model, DLQ, retention period,
-- replay configuration, or broker-specific retry/backoff behavior.
--
-- CloudEvent identity is (event_source, event_id).
-- outbox_id is persistence identity only and MUST NOT substitute for event_id.

CREATE TABLE atlazora_outbox (
    outbox_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    event_id uuid NOT NULL,
    event_source text NOT NULL,
    event_type text NOT NULL,
    event_time timestamptz NOT NULL,
    data_schema text NOT NULL,
    envelope jsonb NOT NULL,

    state text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    lease_owner text NULL,
    lease_until timestamptz NULL,

    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at timestamptz NULL,

    CONSTRAINT atlazora_outbox_event_identity_unique
        UNIQUE (event_source, event_id),

    CONSTRAINT atlazora_outbox_event_source_nonempty_check
        CHECK (btrim(event_source) <> ''),

    CONSTRAINT atlazora_outbox_event_type_nonempty_check
        CHECK (btrim(event_type) <> ''),

    CONSTRAINT atlazora_outbox_data_schema_nonempty_check
        CHECK (btrim(data_schema) <> ''),

    CONSTRAINT atlazora_outbox_state_check
        CHECK (state IN ('pending', 'processing', 'published')),

    CONSTRAINT atlazora_outbox_attempt_count_check
        CHECK (attempt_count >= 0),

    CONSTRAINT atlazora_outbox_lifecycle_check
        CHECK (
            (
                state = 'pending'
                AND lease_owner IS NULL
                AND lease_until IS NULL
                AND published_at IS NULL
            )
            OR
            (
                state = 'processing'
                AND lease_owner IS NOT NULL
                AND btrim(lease_owner) <> ''
                AND lease_until IS NOT NULL
                AND published_at IS NULL
            )
            OR
            (
                state = 'published'
                AND lease_owner IS NULL
                AND lease_until IS NULL
                AND published_at IS NOT NULL
            )
        )
);

CREATE INDEX atlazora_outbox_pending_scan_idx
    ON atlazora_outbox (available_at, outbox_id)
    WHERE state = 'pending';

CREATE INDEX atlazora_outbox_processing_lease_idx
    ON atlazora_outbox (lease_until, outbox_id)
    WHERE state = 'processing';

-- Foundational idempotent-consumption persistence.
--
-- consumer_name scopes processing ownership to one logical consumer.
-- Duplicate event identity remains CloudEvents (event_source, event_id).
CREATE TABLE atlazora_event_consumption (
    consumer_name text NOT NULL,
    event_source text NOT NULL,
    event_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT atlazora_event_consumption_consumer_name_nonempty_check
        CHECK (btrim(consumer_name) <> ''),

    CONSTRAINT atlazora_event_consumption_event_source_nonempty_check
        CHECK (btrim(event_source) <> ''),

    PRIMARY KEY (consumer_name, event_source, event_id)
);

COMMENT ON TABLE atlazora_outbox IS
    'Transport-independent durable publication intent for W00-WU05 Transactional Outbox.';

COMMENT ON COLUMN atlazora_outbox.outbox_id IS
    'Persistence identity only; distinct from CloudEvent event_id.';

COMMENT ON COLUMN atlazora_outbox.event_id IS
    'CloudEvent id. Runtime producers must use the approved RFC 9562 UUIDv7 convention.';

COMMENT ON COLUMN atlazora_outbox.event_source IS
    'CloudEvent source; together with event_id forms the approved duplicate identity boundary.';

COMMENT ON TABLE atlazora_event_consumption IS
    'Foundational duplicate-consumption record keyed by logical consumer plus CloudEvent source and id.';
