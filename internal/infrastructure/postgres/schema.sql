CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer_group   TEXT NOT NULL,
    topic            TEXT NOT NULL,
    partition        INT  NOT NULL,
    committed_offset BIGINT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);
