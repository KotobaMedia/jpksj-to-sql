CREATE EXTENSION IF NOT EXISTS "postgis";

CREATE TABLE IF NOT EXISTS "datasets" (
    "table_name" TEXT PRIMARY KEY NOT NULL,
    "metadata" JSONB NOT NULL
    -- "extents": GEOMETRY
);

CREATE TABLE IF NOT EXISTS "admini_boundary_cd" (
    "行政区域コード" VARCHAR(5) PRIMARY KEY NOT NULL,
    "都道府県名（漢字）" TEXT,
    "市区町村名（漢字）" TEXT,
    "都道府県名（カナ）" TEXT,
    "市区町村名（カナ）" TEXT,
    "コードの改定区分" TEXT,
    "改正年月日" TEXT,
    "改正後のコード" VARCHAR(5) NOT NULL,
    "改正後の名称" TEXT,
    "改正後の名称（カナ）" TEXT,
    "改正事由等" TEXT
);
