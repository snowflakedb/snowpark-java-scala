#!/bin/bash -e
#
# Prints (or copies to $1) the exact-runtime metadata derived from `jdbcVersionExact` in build.sbt:
#
#   JDBC_GAV=net.snowflake:snowflake-jdbc[-fips]:<version>
#   JDBC_SHA256=<sha-256 of the resolved JDBC jar>
#
# This is the same content that is packaged in the Snowpark jar at
# META-INF/snowflake/snowpark-exact-runtime.properties. Adapter and RPM automation should consume
# it instead of hard-coding a JDBC version; `jdbcVersionExact` is the only hand-edited JDBC version.
#
# Usage: scripts/exact-runtime-metadata.sh [OUTPUT_FILE]
# Set SNOWPARK_FIPS=true for the FIPS artifact. Only KEY=VALUE lines go to stdout (or OUTPUT_FILE).

cd "$(dirname "$0")/.."
sbt -error exportExactRuntimeMetadata >&2
metadata=target/exact-runtime/snowpark-exact-runtime.properties
if [ -n "$1" ]; then
  cp "$metadata" "$1"
else
  cat "$metadata"
fi
