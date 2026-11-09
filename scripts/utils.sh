#!/usr/bin/env bash
set -Eexo pipefail

cleanup() {
  rm -f profile.properties
  git checkout -- .
}

trap cleanup SIGINT SIGTERM ERR EXIT

decrypt_profile_properties_gpg() {
  # disable xtrace so GPG_KEY passphrase is not echoed to logs
  set +x
  if [ -z "$GPG_KEY" ]; then
    echo '[ERROR] GPG_KEY was not defined!'
    exit 1
  fi

  if [ -f "$1" ]; then
    echo "[INFO] Decrypting profile.properties file: $1"
    gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties $1
  else
    echo "[ERROR] GPG encrypted profile.properties file not found at: $1"
  fi
  # re-enable xtrace now that the passphrase is no longer referenced
  set -x
}

# The system sbt runner on SnowOS Jenkins nodes downloads sbt-launch.jar from
# Maven Central on every fresh workspace, and Maven Central rate-limits (HTTP
# 429) the shared Jenkins egress IP (see SNOW-4292027). The runner's silent
# curl then fails with "shasum mismatch". Pre-fetch the launcher into the
# runner's cache, preferring Snowflake Artifactory, with retries and visible
# HTTP status codes.
SBT_ARTIFACTORY_REPO="https://artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/artifactory/development-maven-virtual"
SBT_CENTRAL_REPO="https://repo1.maven.org/maven2"

prefetch_sbt_launcher() {
  local sbt_script version dest repo repos
  sbt_script="$(command -v sbt)" || { echo "[ERROR] sbt not found on PATH"; return 1; }
  version="$(sed -n 's/^declare init_sbt_version=\([0-9][0-9.]*\).*/\1/p' "$sbt_script" | head -1)"
  # DEBUG(remove before merge): confirm assumptions about the Jenkins node.
  echo "[DEBUG] sbt script: $sbt_script; launcher version: '${version}'; HOME=$HOME"
  grep -n '^declare init_sbt_version=\|\.cache/sbt/boot/sbt-launch' "$sbt_script" || true
  if [ -z "$version" ]; then
    echo "[WARN] Could not determine sbt launcher version from $sbt_script; skipping prefetch"
    return 0
  fi
  dest="$HOME/.cache/sbt/boot/sbt-launch/$version/sbt-launch-$version.jar"
  if [ -f "$dest" ]; then
    echo "[INFO] sbt launcher $version already cached at $dest"
    return 0
  fi
  mkdir -p "$(dirname "$dest")"
  if [ -n "$SBT_LAUNCH_REPO" ]; then
    repos="$SBT_LAUNCH_REPO"
  else
    repos="$SBT_ARTIFACTORY_REPO $SBT_CENTRAL_REPO"
  fi
  for repo in $repos; do
    if download_sbt_launcher "$repo" "$version" "$dest"; then
      return 0
    fi
  done
  echo "[ERROR] Failed to download sbt launcher $version from: $repos"
  return 1
}

# download_sbt_launcher <repo> <version> <dest>: download and sha1-verify the
# launcher jar from one Maven repo, retrying SBT_PREFETCH_ATTEMPTS times.
download_sbt_launcher() {
  local repo="$1" version="$2" dest="$3" url code attempt expected actual
  local attempts="${SBT_PREFETCH_ATTEMPTS:-3}" backoff="${SBT_PREFETCH_BACKOFF:-10}"
  url="$repo/org/scala-sbt/sbt-launch/$version/sbt-launch-$version.jar"
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    code="$(curl -sSL -m 120 --retry 2 -w '%{http_code}' -o "$dest.sha1" "$url.sha1" || true)"
    expected="$({ tr -d '[:space:]' < "$dest.sha1"; } 2>/dev/null | cut -c1-40 || true)"
    if [ "$code" = "200" ] && [[ "$expected" =~ ^[0-9a-f]{40}$ ]]; then
      code="$(curl -sSL -m 300 --retry 2 -w '%{http_code}' -o "$dest.temp" "$url" || true)"
      actual="$( (sha1sum "$dest.temp" 2>/dev/null || shasum -a 1 "$dest.temp") | cut -d' ' -f1 || true)"
      if [ "$code" = "200" ] && [ "$actual" = "$expected" ]; then
        mv "$dest.temp" "$dest"
        rm -f "$dest.sha1"
        echo "[INFO] Downloaded sbt launcher $version from $repo (attempt $attempt)"
        return 0
      fi
      echo "[WARN] $url attempt $attempt: HTTP $code, sha1 expected=$expected actual=$actual"
    else
      echo "[WARN] $url.sha1 attempt $attempt: HTTP $code, body starts: $(head -c 200 "$dest.sha1" 2>/dev/null | tr -d '\n')"
    fi
    rm -f "$dest.temp" "$dest.sha1"
    if [ "$attempt" -lt "$attempts" ]; then
      sleep $((attempt * backoff))
    fi
  done
  return 1
}

run_test_suites() {
  prefetch_sbt_launcher

  if [ -z "$1" ]; then
    echo "[INFO] Setting system JVM timezone: $1"
    export TZ="$1"
  else
    DEFAULT_TZ="America/Los_Angeles"
    echo "[INFO] Setting system JVM timezone: $DEFAULT_TZ"
    export TZ="$DEFAULT_TZ"
  fi

  # See com.snowflake.snowpark.Session companion object.
  # Avoid failures in subsequent test runs due to an already closed stderr.
  export DISABLE_REDIRECT_STDERR=""

  # Set JVM system property for FIPS test if SNOWPARK_FIPS is true.
  if [ "$SNOWPARK_FIPS" = true ]; then
    FIPS='-J-DFIPS_TEST=true'
    echo "Passing $FIPS to sbt"
  else
    FIPS=''
  fi

  # test
  sbt $FIPS clean +compile \
    +JavaAPITests:test \
    +NonparallelTests:test \
    '++ 2.12.20 OtherTests:testOnly * -- -l SampleDataTest' \
    '++ 2.13.16 OtherTests:testOnly * -- -l SampleDataTest' \
    '++ 2.12.20 UDFTests:testOnly * -- -l SampleDataTest' \
    '++ 2.13.16 UDFTests:testOnly * -- -l SampleDataTest' \
    '++ 2.12.20 UDTFTests:testOnly * -- -l SampleDataTest' \
    '++ 2.13.16 UDTFTests:testOnly * -- -l SampleDataTest -l com.snowflake.snowpark.UDFPackageTest' \
    +UDAFTests:test \
    +SprocTests:test
}
