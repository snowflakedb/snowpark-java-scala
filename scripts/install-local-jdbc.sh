#!/bin/bash -e
#
# Creates an isolated local Maven repository holding one JDBC driver jar together with JDBC's
# public_pom.xml, for LOCAL pre-release validation of `jdbcVersionExact` (see CONTRIBUTING.md).
#
# Do not use ~/.m2: a locally installed JDBC snapshot has the JDBC *build* POM, which pulls in
# internal transitive jars and changes Snowpark's own dependency versions. The public POM is what
# Maven Central publishes, so the resulting classpath is representative.
#
# Usage:
#   scripts/install-local-jdbc.sh --repo DIR --version VERSION --jar JAR --pom PUBLIC_POM [--fips]
#
#   --repo DIR       new or existing isolated repository directory (must not be ~/.m2/repository)
#   --version V      version to install as, e.g. 4.4.2-SNAPSHOT
#   --jar JAR        the driver jar (snowflake-jdbc.jar, or the FIPS jar with --fips)
#   --pom PUBLIC_POM public_pom.xml (FIPS/public_pom.xml with --fips) from the JDBC repository
#   --fips           install as snowflake-jdbc-fips
#
# Then, after `sbt clean` (so that sbt cannot reuse a stale update cache):
#   sbt -Dsnowpark.jdbcVersionExact=V -Dmaven.repo.local=DIR "++ 2.12.20 jdbc4Compat/test"

repo=; version=; jar=; pom=; artifact=snowflake-jdbc
while [ $# -gt 0 ]; do
  case "$1" in
    --repo) repo="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --jar) jar="$2"; shift 2 ;;
    --pom) pom="$2"; shift 2 ;;
    --fips) artifact=snowflake-jdbc-fips; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
if [ -z "$repo" ] || [ -z "$version" ] || [ -z "$jar" ] || [ -z "$pom" ]; then
  sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 1
fi
[ -f "$jar" ] || { echo "[ERROR] jar not found: $jar" >&2; exit 1; }
[ -f "$pom" ] || { echo "[ERROR] POM not found: $pom" >&2; exit 1; }

mkdir -p "$repo"
if [ "$(cd "$repo" && pwd -P)" = "$(cd "$HOME/.m2/repository" 2>/dev/null && pwd -P)" ]; then
  echo "[ERROR] --repo must be an isolated directory, not ~/.m2/repository" >&2
  exit 1
fi
if ! grep -q "<artifactId>$artifact</artifactId>" "$pom"; then
  echo "[ERROR] $pom is not the public POM of $artifact" >&2
  exit 1
fi
if grep -q "<parent>" "$pom"; then
  echo "[ERROR] $pom has a parent; this is the build POM, not public_pom.xml" >&2
  exit 1
fi

dir="$repo/net/snowflake/$artifact/$version"
rm -rf "$dir"
mkdir -p "$dir"
cp "$jar" "$dir/$artifact-$version.jar"
# the public POM has a placeholder project version; the first <version> is the project's own
sed "0,/<version>[^<]*<\/version>/s//<version>$version<\/version>/" "$pom" > "$dir/$artifact-$version.pom"
echo "Installed $artifact:$version into $repo"
echo "sha256: $(sha256sum "$dir/$artifact-$version.jar" | cut -d' ' -f1)"
