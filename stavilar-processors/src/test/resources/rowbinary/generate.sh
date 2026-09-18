#!/usr/bin/env bash
# Regenerates expected/<name>.hex from cases.tsv using clickhouse-local from the official image.
# Each line of cases.tsv: <name> TAB <ClickHouse type> TAB <SQL expression>.
# Run from this directory: ./generate.sh [image]
set -euo pipefail
IMAGE="${1:-clickhouse/clickhouse-server:26.8}"
cd "$(dirname "$0")"
mkdir -p expected
while IFS=$'\t' read -r name type expr; do
  [ -z "$name" ] && continue
  docker run --rm "$IMAGE" clickhouse-local -q "SELECT $expr FORMAT RowBinary" | od -An -v -tx1 | tr -d ' \n' > "expected/$name.hex"
  printf '%-26s %s\n' "$name" "$(cat "expected/$name.hex")"
done < cases.tsv
