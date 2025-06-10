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
# 3次医療圏 (都府県＋北海道6圏)
ogr2ogr -f FlatGeobuf a38_3.fgb PG:"$PG_CONN_STR" a38c
# 2次医療圏 (3次より細かい、簡易的な集計データも付与)
scripts/ogr_groupby.sh ./a38_2.fgb "二次医療圏コード" a38b
# ogr2ogr -f FlatGeobuf a38_2.fgb PG:"$PG_CONN_STR" a38b
# 1次医療圏 (2次より更に細かい)
ogr2ogr -f FlatGeobuf a38_1.fgb PG:"$PG_CONN_STR" a38a
```

タイル化

```
tippecanoe -n "医療圏" -N "3次、2次、1次医療圏のポリゴンデータ" -A "<a href=\"https://nlftp.mlit.go.jp/ksj/other/agreement_01.html\">「国土数値情報(医療圏データ)」(国土交通省)</a>をもとに<a href=\"https://kotobamedia.com\">KotobaMedia株式会社</a>作成" -Z0 -z13 -o a38.pmtiles -Ltier1:./a38_1.fgb -Ltier2:./a38_2.fgb -Ltier3:./a38_3.fgb
```
