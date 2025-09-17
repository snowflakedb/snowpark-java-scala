#!/usr/bin/env bash
set -Eexo pipefail

cleanup() {
  rm -f profile.properties
  git checkout -- .
}

trap cleanup SIGINT SIGTERM ERR EXIT

build_and_use_latest_jdbc() {
  git clone git@github.com:snowflakedb/snowflake-jdbc.git
  cd snowflake-jdbc
  echo '[INFO] Building jdbc ...'
  ./mvnw clean verify >& jdbc.build.out
  tail jdbc.build.out
  ./mvnw org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file -Dfile=target/snowflake-jdbc.jar -DpomFile=./public_pom.xml
  cd ..
  ls snowflake-jdbc/target/snowflake-jdbc*.jar
  # snowflake-jdbc/target/snowflake-jdbc.jar is the compiled jar file
  JDBC_SEMVER_PATTERN='val jdbcVersion = "([0-9]+\.[0-9]+\.[0-9]+)"'
  if grep -E -q "$JDBC_SEMVER_PATTERN" build.sbt; then
    # The sed command on mac is different, below is the command on mac
    if [ "$(uname)" == 'Darwin' ]; then
      sed -E -i '' "s/$JDBC_SEMVER_PATTERN/val jdbcVersion = \"1.0-SNAPSHOT\"/" build.sbt
    else
      sed -E -i "s/$JDBC_SEMVER_PATTERN/val jdbcVersion = \"1.0-SNAPSHOT\"/" build.sbt
    fi
  else
    echo "[ERROR] Could not find JDBC version specification in build.sbt: $JDBC_SEMVER_PATTERN"
    exit 1
  fi
  echo '[INFO] Replaced Maven library dependency in build.sbt with dependency on locally built jar.'
  rm -rf snowflake-jdbc
}

decrypt_profile_properties_gpg() {
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
    '++ 2.13.16 UDFTests:testOnly * -- -l SampleDataTest -l com.snowflake.snowpark.UDFPackageTest' \
    '++ 2.12.20 UDTFTests:testOnly * -- -l SampleDataTest' \
    '++ 2.13.16 UDTFTests:testOnly * -- -l SampleDataTest -l com.snowflake.snowpark.UDFPackageTest' \
    +SprocTests:test
}
