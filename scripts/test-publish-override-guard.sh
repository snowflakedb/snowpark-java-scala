#!/bin/bash -e
#
# Checks that every release publishing task refuses to run while -Dsnowpark.jdbcVersionExact
# overrides the production JDBC version. publishLocalSigned is covered because deploy-common.sh
# stages S3 releases with it (PUBLISH=false). Needs sbt; no JDBC download or credentials needed.
#
# Usage: scripts/test-publish-override-guard.sh [SCALA_VERSION]

scala="${1:-2.12.20}"
cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

for task in publish publishSigned publishLocalSigned; do
  # PUBLISH=false mirrors deploy-common.sh's S3 staging path, which the load-time check does not cover.
  if PUBLISH=false sbt -Dsnowpark.jdbcVersionExact=0.0.0-GUARD-TEST -Dmaven.repo.local="$tmp/repo" \
    "++ $scala $task" > "$tmp/$task.log" 2>&1; then
    cat "$tmp/$task.log" >&2
    echo "[FAIL] $task succeeded with -Dsnowpark.jdbcVersionExact set" >&2
    exit 1
  fi
  if ! grep -q "snowpark.jdbcVersionExact must not be set when publishing" "$tmp/$task.log"; then
    cat "$tmp/$task.log" >&2
    echo "[FAIL] $task failed, but not because of the jdbcVersionExact override guard" >&2
    exit 1
  fi
  echo "[PASS] $task is guarded"
done
