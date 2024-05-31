CREATE TABLE IF NOT EXISTS transactional_outbox (
    id UUID PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    creation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    last_attempt_date TIMESTAMP WITH TIME ZONE,
    completion_date TIMESTAMP WITH TIME ZONE,
    attempts INT NOT NULL,
    event TEXT NOT NULL,
    last_error TEXT
);