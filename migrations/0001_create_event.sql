CREATE TABLE event (
 id UUID NOT NULL,
 key varchar(255) NOT NULL,
 value double precision NOT NULL,
 event_time timestamptz NOT NULL,
 ingested_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (id),
CONSTRAINT event_key_not_empty CHECK (key <> '')
);
