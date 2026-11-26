#!/bin/bash -e
#
# Tests scripts/install-local-jdbc.sh with synthetic jar/POM files (no network, no JDBC needed).
# Usage: scripts/test-install-local-jdbc.sh
# The test uses POSIX command-line tools, so it runs on macOS as well as Linux.

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/install-local-jdbc.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fail() { echo "[FAIL] $*" >&2; exit 1; }

echo fake-jar > "$tmp/driver.jar"

write_pom() { # artifact, placeholder, [comment-before-version]
  local comment=
  if [ "${3:-}" = comment-before-version ]; then
    comment='  <!-- <version>COMMENT</version> must not be treated as /project/version. -->'
  fi
  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>net.snowflake</groupId>
  <artifactId>$1</artifactId>
$comment
  <version>$2</version>
  <dependencies>
    <dependency>
      <artifactId>other</artifactId>
      <version>9.9.9</version>
    </dependency>
  </dependencies>
</project>
EOF
}

check_install() { # name, artifact, placeholder, version, layout, extra args...
  local name="$1" artifact="$2" placeholder="$3" version="$4" layout="$5"
  shift 5
  write_pom "$artifact" "$placeholder" "$layout" > "$tmp/$name.pom"
  "$script" --repo "$tmp/$name-repo" --version "$version" --jar "$tmp/driver.jar" \
    --pom "$tmp/$name.pom" "$@" > "$tmp/$name.out" || { cat "$tmp/$name.out" >&2; fail "$name: install failed"; }
  local installed="$tmp/$name-repo/net/snowflake/$artifact/$version/$artifact-$version"
  [ -f "$installed.jar" ] || fail "$name: jar not installed"
  grep -q "^[[:space:]]*<version>$version</version>[[:space:]]*$" "$installed.pom" ||
    fail "$name: project version not replaced"
  # only the first <version> may change: the dependency version must be untouched
  grep -q '<version>9.9.9</version>' "$installed.pom" || fail "$name: dependency version changed"
  [ "$(grep -F -o "<version>$version</version>" "$installed.pom" | wc -l | tr -d ' ')" = 1 ] ||
    fail "$name: more than one version replaced"
  grep -q '^sha256: [0-9a-f]\{64\}$' "$tmp/$name.out" || fail "$name: no sha256 reported"
}

check_install multiline snowflake-jdbc "PLACEHOLDER" 4.4.2-SNAPSHOT plain
check_install comment snowflake-jdbc "PLACEHOLDER" 4.4.2-SNAPSHOT comment-before-version
grep -q '<!-- <version>COMMENT</version> must not be treated' \
  "$tmp/comment-repo/net/snowflake/snowflake-jdbc/4.4.2-SNAPSHOT/snowflake-jdbc-4.4.2-SNAPSHOT.pom" ||
  fail "XML comment was modified"
check_install fips snowflake-jdbc-fips "99.99.99" 4.4.2-SNAPSHOT plain --fips

# A dependency artifactId must not make a POM with the wrong project artifactId pass validation.
write_pom snowflake-jdbc X plain |
  sed 's|<artifactId>other</artifactId>|<artifactId>snowflake-jdbc-fips</artifactId>|' \
    > "$tmp/wrong.pom"
if "$script" --repo "$tmp/wrong-repo" --version 1 --jar "$tmp/driver.jar" --pom "$tmp/wrong.pom" --fips \
  > /dev/null 2>&1; then
  fail "wrong artifact POM was accepted"
fi

# a build POM with a parent is rejected
sed 's|<modelVersion>|<parent><artifactId>p</artifactId></parent><modelVersion>|' \
  "$tmp/multiline.pom" > "$tmp/parent.pom"
if "$script" --repo "$tmp/parent-repo" --version 1 --jar "$tmp/driver.jar" --pom "$tmp/parent.pom" \
  > /dev/null 2>&1; then
  fail "POM with a parent was accepted"
fi

# XML/path metacharacters in a version are rejected instead of producing malformed coordinates.
if "$script" --repo "$tmp/unsafe-repo" --version '4.4.2&bad' --jar "$tmp/driver.jar" \
  --pom "$tmp/multiline.pom" > /dev/null 2>&1; then
  fail "unsafe version was accepted"
fi

# ~/.m2/repository is rejected
mkdir -p "$tmp/home/.m2/repository"
if HOME="$tmp/home" "$script" --repo "$tmp/home/.m2/repository" --version 1 --jar "$tmp/driver.jar" \
  --pom "$tmp/multiline.pom" > /dev/null 2>&1; then
  fail "~/.m2/repository was accepted"
fi

echo "[PASS] install-local-jdbc.sh"
