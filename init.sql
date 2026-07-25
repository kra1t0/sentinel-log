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

-- Dynamic Rules Config Table
CREATE TABLE IF NOT EXISTS tenant_rules (
    rule_id UUID PRIMARY KEY DEFAULT get_random_uuid(),
    tenant_id VARCHAR(100) NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    is_enabled BOOLEAN DEFAULT TRUE,

    -- Velocity Limits
    time_window_seconds INT NOT NULL DEFAULT 30,
    max_events_allowed INT NOT NULL DEFAULT 10,
    cooldown_seconds INT NOT NULL DEFAULT 300,
    severity VARCHAR(20) DEFAULT 'HIGH',

    -- Dimensions & Filters
    group_by_field VARCHAR(50) DEFAULT 'actor_ip',

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

--Optimze queries with indexing..
CREATE INDEX IF NOT EXISTS idx_logs_tenant_timestamp ON security_logs(tenant_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_actor_ip_timestamp ON security_logs(actor_ip, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_event_type ON security_logs(event_type);


-- RLS Row level security
ALTER TABLE security_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_rules ENABLE ROW LEVEL SECURITY;

-- Defailt seed rule
INSERT INTO tenant_rules(tenant_id, rule_name, event_type, time_window_seconds, max_events_allowed, cooldown_seconds, severity, group_by_field)
VALUES('tenant_acme_corp', 'Brute Force Defense', 'login_failed', 30, 5, 300, 'CRITICAL', 'actor_ip')
ON CONFLICT DO NOTHING;
