CREATE TABLE IF NOT EXISTS "node_metrics_v2" (
    "cluster" TEXT,
    "metric_domain" TEXT,
    "metric_type" TEXT,
    "host" TEXT,
    "metric_scope" TEXT,
    "metric_name" TEXT,
    "ts" TIMESTAMP WITH TIME ZONE,
    "metric_attribute" TEXT,
    "value" DOUBLE PRECISION,
    PRIMARY KEY ("cluster", "host", "metric_domain", "metric_type", "metric_scope", "metric_name", "metric_attribute", "ts")
);

CREATE TABLE IF NOT EXISTS "node_operations" (
    "cluster" TEXT,
    "type" TEXT,
    "host" TEXT,
    "ts" TIMESTAMP WITH TIME ZONE,
    "data" TEXT,
    PRIMARY KEY ("cluster", "type", "host", "ts")
);
