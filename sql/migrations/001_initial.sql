-- +goose Up
CREATE TABLE IF NOT EXISTS messages (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    source      TEXT NOT NULL,
    body        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_event_type ON messages (event_type);
CREATE INDEX idx_messages_created_at ON messages (created_at);

-- +goose Down
DROP TABLE IF EXISTS messages;
