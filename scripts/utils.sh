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

run_test_suites() {
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
