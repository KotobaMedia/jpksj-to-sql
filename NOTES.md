# `A04` - 津波浸水深

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

CREATE OR REPLACE VIEW a40_normalized AS
SELECT
   t.ogc_fid,
   t.都道府県コード,
   t.都道府県名,
   a40_normalize_range(t."津波浸水深の区分") AS "津波浸水深の区分",
   t.geom
FROM a40 t;
```

FlatGeobuf へのエクスポート

```
ogr2ogr -f FlatGeobuf a40_normalized.fgb PG:"..." a40_normalized
```
