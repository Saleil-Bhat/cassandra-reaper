-- H2-compatible version of multi-instance reaper Postgres DDL
-- CHANGES:
--     TEXT  --> VARCHAR(255)  because H2 doesn't support index on TEXT

CREATE TABLE IF NOT EXISTS "leader" (
  "leader_id" BIGINT PRIMARY KEY,
  "reaper_instance_id" BIGINT,
  "reaper_instance_host" TEXT,
  "last_heartbeat" TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS "running_reapers" (
  "reaper_instance_id" BIGINT PRIMARY KEY,
  "reaper_instance_host" TEXT,
  "last_heartbeat" TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS "node_metrics_v1" (
  "run_id"                  BIGINT,
  "ts"                      TIMESTAMP WITH TIME ZONE,
  "node"                    VARCHAR(255),
  "cluster"                 TEXT,
  "datacenter"              TEXT,
  "requested"               BOOLEAN,
  "pending_compactions"     INT,
  "has_repair_running"      BOOLEAN,
  "active_anticompactions"  INT,
  PRIMARY KEY("run_id", "ts", "node")
);

--- Sidecar mode

CREATE TABLE IF NOT EXISTS "node_operations" (
    "cluster" VARCHAR(255),
    "type" VARCHAR(255),
    "host" VARCHAR(255),
    "ts" TIMESTAMP WITH TIME ZONE,
    "data" TEXT,
    PRIMARY KEY ("cluster", "type", "host", "ts")
);
