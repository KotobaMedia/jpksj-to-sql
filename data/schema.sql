CREATE EXTENSION IF NOT EXISTS "postgis";

CREATE TABLE IF NOT EXISTS "datasets" (
    "table_name" TEXT PRIMARY KEY NOT NULL,
    "metadata" JSONB NOT NULL
    -- "extents": GEOMETRY
);
