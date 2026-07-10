CREATE TABLE IF NOT EXISTS telemetry (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    patient_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL,
    spo2 INTEGER,
    temperature DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_patient_timestamp
    ON telemetry (patient_id, "timestamp");

CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    patient_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT alerts_dedup_key_unique UNIQUE (dedup_key)
);

ALTER TABLE alerts
    ADD COLUMN IF NOT EXISTS dedup_key TEXT;

UPDATE alerts
SET dedup_key = patient_id || ':' || alert_type || ':' || FLOOR(EXTRACT(EPOCH FROM detected_at) / 300)::TEXT
WHERE dedup_key IS NULL;

ALTER TABLE alerts
    ALTER COLUMN dedup_key SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'alerts_dedup_key_unique'
          AND conrelid = 'alerts'::regclass
    ) THEN
        ALTER TABLE alerts
            ADD CONSTRAINT alerts_dedup_key_unique UNIQUE (dedup_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_alerts_patient_created_at
    ON alerts (patient_id, created_at);
