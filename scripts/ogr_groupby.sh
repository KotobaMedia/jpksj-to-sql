#!/bin/bash

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <output.fgb> <group_field> <layer_name>"
  exit 1
fi

OUT_FGB="$1"
GROUP_FIELD="$2"
LAYER="$3"

# Ensure PG_CONN_STR is defined
if [ -z "${PG_CONN_STR:-}" ]; then
  echo "Error: PG_CONN_STR environment variable is not set"
  exit 1
fi

# Extract field names using JSON output and jq
FIELDS=$(ogrinfo -ro -so -json PG:"$PG_CONN_STR" "$LAYER" \
    | jq -r '.layers[0].fields[] | select(.type != "Geometry" and .name != "'"$GROUP_FIELD"'") | .name' \
    | tr '\n' ',' | sed 's/,$//')

echo $FIELDS

# Build the SELECT clause (GROUP_FIELD must be first)
SQL="SELECT $GROUP_FIELD, $FIELDS, ST_Union(geom) AS geom FROM \"$LAYER\" GROUP BY $GROUP_FIELD"

# Run the query
ogr2ogr -f FlatGeobuf "$OUT_FGB" PG:"$PG_CONN_STR" -dialect sqlite -sql "$SQL"
