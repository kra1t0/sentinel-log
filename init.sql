-- Enable UUID gen
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--Core security Telemetry table matching the data blueprints
CREATE TABLE IF NOT EXISTS security_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id VARCHAR(100) NOT NULL,
  event_source VARCHAR(100) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  actor_ip INET NOT NULL,
  metadata JSONB DEFAULT '{}'::jsonb,
  timestamp TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

--Optimze queries with indexing..
CREATE INDEX IF NOT EXISTS idx_logs_tenant_timestamp ON security_logs(tenant_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_actor_ip_timestamp ON security_logs(actor_ip, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_event_type ON security_logs(event_type);


-- RLS Row level security
ALTER TABLE security_logs ENABLE ROW LEVEL SECURITY;
