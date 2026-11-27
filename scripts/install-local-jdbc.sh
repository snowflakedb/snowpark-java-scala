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
# Then, after `sbt clean jdbc4Compat/clean` (so neither project reuses a stale update cache):
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
case "$version" in
  *[!A-Za-z0-9._+-]*)
    echo "[ERROR] version contains characters unsafe for Maven coordinates or XML: $version" >&2
    exit 1
    ;;
esac

mkdir -p "$repo"
if [ "$(cd "$repo" && pwd -P)" = "$(cd "$HOME/.m2/repository" 2>/dev/null && pwd -P)" ]; then
  echo "[ERROR] --repo must be an isolated directory, not ~/.m2/repository" >&2
  exit 1
fi

dir="$repo/net/snowflake/$artifact/$version"
rm -rf "$dir"
mkdir -p "$dir"
cp "$jar" "$dir/$artifact-$version.jar"
# Replace only /project/version in a parent-free public POM. The root group/artifact checks prevent
# a comment or dependency element from being mistaken for project coordinates. Comments (including
# same-line and multiline ones) are ignored both when deciding that a line is the root version and
# when locating the text to replace. POSIX awk is used because the GNU-only `0,/re/` sed address
# silently does nothing on macOS /usr/bin/sed.
out="$dir/$artifact-$version.pom"
if ! POM_VERSION="$version" POM_ARTIFACT="$artifact" awk '
  function without_comments(text, start, rest, finish) {
    while (1) {
      if (in_comment) {
        finish = index(text, "-->")
        if (!finish) return ""
        text = substr(text, finish + 3)
        in_comment = 0
      }
      start = index(text, "<!--")
      if (!start) return text
      rest = substr(text, start + 4)
      finish = index(rest, "-->")
      if (!finish) {
        in_comment = 1
        return substr(text, 1, start - 1)
      }
      text = substr(text, 1, start - 1) substr(rest, finish + 3)
    }
  }
  # 1-based offset of the first <version> that is outside any XML comment, given whether the line
  # starts inside a comment; 0 if there is none.
  function version_offset(text, commented, offset, start, vstart, finish) {
    offset = 0
    while (text != "") {
      if (commented) {
        finish = index(text, "-->")
        if (!finish) return 0
        offset += finish + 2
        text = substr(text, finish + 3)
        commented = 0
        continue
      }
      start = index(text, "<!--")
      vstart = index(text, "<version>")
      if (vstart && (!start || vstart < start)) return offset + vstart
      if (!start) return 0
      offset += start + 3
      text = substr(text, start + 4)
      commented = 1
    }
    return 0
  }
  {
    line_in_comment = in_comment
    visible = without_comments($0)
    if (!in_project && visible ~ /^[[:space:]]*<project([[:space:]>])/) in_project = 1
    if (in_project && !header_done) {
      if (visible ~ /^[[:space:]]*<parent([[:space:]>])/) invalid_parent = 1
      if (visible ~ /^[[:space:]]*<groupId>[^<]*<\/groupId>[[:space:]]*$/) {
        value = visible
        sub(/^[[:space:]]*<groupId>/, "", value)
        sub(/<\/groupId>[[:space:]]*$/, "", value)
        group_ok = (value == "net.snowflake")
      }
      if (visible ~ /^[[:space:]]*<artifactId>[^<]*<\/artifactId>[[:space:]]*$/) {
        value = visible
        sub(/^[[:space:]]*<artifactId>/, "", value)
        sub(/<\/artifactId>[[:space:]]*$/, "", value)
        artifact_ok = (value == ENVIRON["POM_ARTIFACT"])
      }
      if (!version_done && group_ok && artifact_ok &&
          visible ~ /^[[:space:]]*<version>[^<]*<\/version>[[:space:]]*$/) {
        pos = version_offset($0, line_in_comment)
        tail = substr($0, pos)
        if (pos && match(tail, /^<version>[^<]*<\/version>/)) {
          before = substr($0, 1, pos - 1)
          after = substr(tail, RLENGTH + 1)
          $0 = before "<version>" ENVIRON["POM_VERSION"] "</version>" after
          version_done = 1
        }
      }
      if (visible ~ /^[[:space:]]*<(dependencies|dependencyManagement|build|profiles)([[:space:]>])/)
        header_done = 1
    }
    print
  }
  END { if (invalid_parent || !group_ok || !artifact_ok || !version_done) exit 3 }
' "$pom" > "$out"; then
  echo "[ERROR] $pom must be a parent-free public POM for net.snowflake:$artifact" >&2
  rm -rf "$dir"
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha=$(sha256sum < "$dir/$artifact-$version.jar" | cut -d' ' -f1)
else
  sha=$(shasum -a 256 < "$dir/$artifact-$version.jar" | cut -d' ' -f1)
fi
echo "Installed $artifact:$version into $repo"
echo "sha256: $sha"
