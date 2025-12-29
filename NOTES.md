```
$PG_CONN_STR="host=..."
```

# `A40` - 津波浸水深

「津波浸水深の区分」のフォーマットが統一化されていないためこちらで正規化しています。
そのうち組み込むかも。

```sql
CREATE OR REPLACE FUNCTION a40_normalize_range(range_text text)
RETURNS text AS $$
DECLARE
  norm text;
  lower_range text;
  upper_range text;
BEGIN
  -- Step 1: Trim whitespace.
  norm := trim(range_text);

  -- Normalize A - B patterns
  norm := regexp_replace(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]+)([0-9\.]+)m?(?:未満)?$', '\1m-\2m');
  -- A-
  norm := regexp_replace(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]*)$', '\1m-');
  -- -B
  norm := regexp_replace(norm, '^(?:[ ～]*)([0-9\.]+)m?(?:未満)?$', '-\1m');

  -- Replace .0
  norm := regexp_replace(norm, '(\d+)\.0m', '\1m', 'g');

  RETURN norm;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION a40_get_upper_bound(range_text text)
RETURNS numeric AS $$
DECLARE
    norm text;
    match_result text[];
BEGIN
    -- Step 1: Trim whitespace.
    norm := trim(range_text);

    -- Case 1: A - B pattern (e.g., "0.5m - 1.0m未満")
    match_result := regexp_match(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]+)([0-9\.]+)m?(?:未満)?$');
    IF match_result IS NOT NULL THEN
    RETURN match_result[2]::numeric;
    END IF;

    -- Case 2: A- pattern (e.g., "5m以上")
    match_result := regexp_match(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]*)$');
    IF match_result IS NOT NULL THEN
    RETURN 99; -- Special value for unspecified upper bound
    END IF;

    -- Case 3: -B pattern (e.g., "0.3m未満")
    match_result := regexp_match(norm, '^(?:[ ～]*)([0-9\.]+)m?(?:未満)?$');
    IF match_result IS NOT NULL THEN
    RETURN match_result[1]::numeric;
    END IF;

    -- Fallback for unparseable input
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION a40_get_lower_bound(range_text text)
RETURNS numeric AS $$
DECLARE
    norm text;
    match_result text[];
BEGIN
    -- Step 1: Trim whitespace.
    norm := trim(range_text);

    -- Case 1: A - B pattern (e.g., "0.5m - 1.0m未満")
    match_result := regexp_match(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]+)([0-9\.]+)m?(?:未満)?$');
    IF match_result IS NOT NULL THEN
    RETURN match_result[1]::numeric;
    END IF;

    -- Case 2: A- pattern (e.g., "5m以上")
    match_result := regexp_match(norm, '^([0-9\.]+)m?(?:以上)?(?:[ ～]*)$');
    IF match_result IS NOT NULL THEN
    RETURN match_result[1]::numeric;
    END IF;

    -- Case 3: -B pattern (e.g., "0.3m未満")
    match_result := regexp_match(norm, '^(?:[ ～]*)([0-9\.]+)m?(?:未満)?$');
    IF match_result IS NOT NULL THEN
    RETURN -99; -- Special value for unspecified lower bound
    END IF;

    -- Fallback for unparseable input
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE VIEW a40_normalized AS
SELECT
   t.ogc_fid,
   t.都道府県コード,
   t.都道府県名,
   a40_normalize_range(t."津波浸水深の区分") AS "津波浸水深の区分",
   a40_get_lower_bound(t."津波浸水深の区分") AS "min",
   a40_get_upper_bound(t."津波浸水深の区分") AS "max",
   t.geom
FROM a40 t;
```

FlatGeobuf へのエクスポート

```
ogr2ogr -f FlatGeobuf a40_normalized.fgb PG:"$PG_CONN_STR" a40_normalized
```

# `A38` - 医療圏

```
cargo run -- --filter-identifiers A38 FlatGeobuf ./tmp/out
```

```
# 2次医療圏 (簡易的な集計データも付与)
# ogr_groupby を利用して島等を同じポリゴンに設定する
scripts/ogr_groupby.sh ./tmp/out/a38b_group.fgb "二次医療圏コード" ./tmp/out/a38b.fgb
```

タイル化

```
tippecanoe -n "医療圏" -N "3次、2次、1次医療圏のポリゴンデータ" -A "<a href=\"https://tiles.kmproj.com/attribution\">Attribution</a>" -Z0 -z13 -o a38.pmtiles -Ltier1:./tmp/out/a38a.fgb -Ltier2:./tmp/out/a38b_group.fgb -Ltier3:./tmp/out/a38c.fgb
```

# `N03` - 行政区域

```
cargo run -- --filter-identifiers N03 FlatGeobuf ./tmp/out
```

タイル化

```
tippecanoe -n "行政区域" -N "都道府県と市区町村ポリゴンデータ" -A "<a href=\"https://tiles.kmproj.com/attribution\">Attribution</a>" --coalesce --use-attribute-for-id="全国地方公共団体コード" -aI -f -Z0 -z13 -o n03.pmtiles -Lcity:./tmp/out/n03.fgb -Lpref:./tmp/out/n03_prefecture.fgb
```
