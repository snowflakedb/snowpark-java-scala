#!/usr/bin/env bash
#
# TEMPORARY: builds com.snowflake:snowflake-session-api:0.1.0-SNAPSHOT from the vendored sources
# in ci/snowflake-session-api and installs it into the local Maven repository, where build.sbt
# resolves it (Resolver.mavenLocal). Delete this script, and its calls, once the session API is
# published. See ci/snowflake-session-api/README.md.
#
# Uses only javac and jar, so it runs on any JDK 8+ without Maven. The build happens outside the
# repository tree, so it leaves `git status` clean. Environment overrides:
#   SESSION_API_JAVA_HOME  JDK used to compile; defaults to a JDK 17+ when the GitHub runner
#                          provides one (javac 17+ reproduces the monorepo Bazel class files), else
#                          the JDK on JAVA_HOME / PATH.
#   SESSION_API_M2_REPO    local Maven repository; defaults to ~/.m2/repository.
set -euo pipefail

GROUP_PATH="com/snowflake"
ARTIFACT="snowflake-session-api"
VERSION="0.1.0-SNAPSHOT"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE_DIR="$REPO_ROOT/ci/$ARTIFACT"
M2_REPO="${SESSION_API_M2_REPO:-$HOME/.m2/repository}"

JAVAC="javac"
JAR="jar"
for candidate in "${SESSION_API_JAVA_HOME:-}" "${JAVA_HOME_21_X64:-}" "${JAVA_HOME_17_X64:-}" \
    "${JAVA_HOME:-}"; do
  if [ -n "$candidate" ] && [ -x "$candidate/bin/javac" ] && [ -x "$candidate/bin/jar" ]; then
    JAVAC="$candidate/bin/javac"
    JAR="$candidate/bin/jar"
    break
  fi
done

# javac 8 has no --release flag; it targets Java 8 natively.
if "$JAVAC" -version 2>&1 | grep -q 'javac 1\.8'; then
  RELEASE_OPTS=(-source 1.8 -target 1.8)
else
  RELEASE_OPTS=(--release 8 -Xlint:-options)
fi

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

echo "[INFO] Building $ARTIFACT $VERSION with $("$JAVAC" -version 2>&1)."
find "$MODULE_DIR/src/main/java" -name '*.java' > "$BUILD_DIR/sources.txt"
mkdir -p "$BUILD_DIR/classes"
# -g -parameters --release 8 matches the monorepo Bazel build of the module.
"$JAVAC" "${RELEASE_OPTS[@]}" -g -parameters -encoding UTF-8 \
  -d "$BUILD_DIR/classes" "@$BUILD_DIR/sources.txt"

ARTIFACT_DIR="$M2_REPO/$GROUP_PATH/$ARTIFACT"
VERSION_DIR="$ARTIFACT_DIR/$VERSION"
mkdir -p "$VERSION_DIR"
"$JAR" cf "$VERSION_DIR/$ARTIFACT-$VERSION.jar" -C "$BUILD_DIR/classes" .
cp "$MODULE_DIR/pom.xml" "$VERSION_DIR/$ARTIFACT-$VERSION.pom"

TIMESTAMP="$(date -u +%Y%m%d%H%M%S)"
cat > "$VERSION_DIR/maven-metadata-local.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<metadata modelVersion="1.1.0">
  <groupId>com.snowflake</groupId>
  <artifactId>$ARTIFACT</artifactId>
  <version>$VERSION</version>
  <versioning>
    <snapshot>
      <localCopy>true</localCopy>
    </snapshot>
    <lastUpdated>$TIMESTAMP</lastUpdated>
    <snapshotVersions>
      <snapshotVersion>
        <extension>jar</extension>
        <value>$VERSION</value>
        <updated>$TIMESTAMP</updated>
      </snapshotVersion>
      <snapshotVersion>
        <extension>pom</extension>
        <value>$VERSION</value>
        <updated>$TIMESTAMP</updated>
      </snapshotVersion>
    </snapshotVersions>
  </versioning>
</metadata>
EOF
cat > "$ARTIFACT_DIR/maven-metadata-local.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.snowflake</groupId>
  <artifactId>$ARTIFACT</artifactId>
  <versioning>
    <versions>
      <version>$VERSION</version>
    </versions>
    <lastUpdated>$TIMESTAMP</lastUpdated>
  </versioning>
</metadata>
EOF

echo "[INFO] Installed $VERSION_DIR/$ARTIFACT-$VERSION.jar"
