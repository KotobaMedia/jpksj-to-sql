#!/bin/bash

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <output.fgb> <group_field> <input_dataset>"
  exit 1
fi

OUT_FGB="$1"
GROUP_FIELD="$2"
INPUT_DATASET="$3"

# Extract field names using JSON output and jq
FIELDS=$(ogrinfo -ro -so -json "$INPUT_DATASET" \
    | jq -r '.layers[0].fields[] | select(.type != "Geometry" and .name != "'"$GROUP_FIELD"'") | .name' \
    | tr '\n' ',' | sed 's/,$//')

echo $FIELDS

# Get the layer name
LAYER=$(ogrinfo -ro -so -json "$INPUT_DATASET" | jq -r '.layers[0].name')

# Build the SELECT clause (GROUP_FIELD must be first)
SQL="SELECT $GROUP_FIELD, $FIELDS, ST_Union(geometry) AS geom FROM \"$LAYER\" GROUP BY $GROUP_FIELD"

# Run the query
ogr2ogr -f FlatGeobuf "$OUT_FGB" "$INPUT_DATASET" -dialect sqlite -sql "$SQL"
