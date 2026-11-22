CREATE TABLE bq_inbox (
    endpoint VARCHAR2(255 BYTE) NOT NULL,
    message_id VARCHAR2(255 BYTE) NOT NULL,
    claimed_ticks NUMBER(19) NOT NULL,
    CONSTRAINT bq_inbox_pk PRIMARY KEY (endpoint, message_id)
);

CREATE TABLE bq_outbox (
    endpoint VARCHAR2(255 BYTE) NOT NULL,
    id VARCHAR2(255 BYTE) NOT NULL,
    enqueue_order NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    correlation_id VARCHAR2(255 BYTE),
    reply_to VARCHAR2(255 BYTE),
    type_name VARCHAR2(255 BYTE) NOT NULL,
    exchange_name VARCHAR2(255 BYTE),
    routing_key VARCHAR2(255 BYTE) NOT NULL,
    mandatory NUMBER(1) NOT NULL CHECK (mandatory IN (0, 1)),
    body BLOB NOT NULL,
    headers BLOB NOT NULL,
    attempts NUMBER(10) DEFAULT 0 NOT NULL,
    retry_after_ticks NUMBER(19) DEFAULT 0 NOT NULL,
    last_error VARCHAR2(1000 BYTE),
    CONSTRAINT bq_outbox_pk PRIMARY KEY (endpoint, id)
);

CREATE INDEX bq_outbox_ready_ix ON bq_outbox (endpoint, enqueue_order);

CREATE TABLE bq_saga (
    endpoint VARCHAR2(255 BYTE) NOT NULL,
    saga_type VARCHAR2(512 BYTE) NOT NULL,
    saga_id VARCHAR2(1024 BYTE) NOT NULL,
    saga_version NUMBER(19) NOT NULL,
    saga_data BLOB NOT NULL,
    CONSTRAINT bq_saga_pk PRIMARY KEY (endpoint, saga_type, saga_id)
);

CREATE TABLE bq_recovery (
    endpoint VARCHAR2(255 BYTE) NOT NULL,
    recovery_id VARCHAR2(80 BYTE) NOT NULL,
    message_id VARCHAR2(255 BYTE) NOT NULL,
    retry_cycle NUMBER(10) NOT NULL,
    is_error NUMBER(1) NOT NULL CHECK (is_error IN (0, 1)),
    error_queue VARCHAR2(255 BYTE),
    exception_type VARCHAR2(1024 BYTE),
    exception_message NCLOB,
    exception_detail NCLOB,
    failed_ticks NUMBER(19) NOT NULL,
    due_ticks NUMBER(19) NOT NULL,
    payload BLOB NOT NULL,
    dispatched NUMBER(1) DEFAULT 0 NOT NULL CHECK (dispatched IN (0, 1)),
    replayed NUMBER(1) DEFAULT 0 NOT NULL CHECK (replayed IN (0, 1)),
    CONSTRAINT bq_recovery_pk PRIMARY KEY (endpoint, recovery_id)
);

CREATE INDEX bq_recovery_due_ix ON bq_recovery (endpoint, dispatched, due_ticks, recovery_id);
CREATE INDEX bq_recovery_error_ix ON bq_recovery (endpoint, is_error, replayed, failed_ticks, recovery_id);
