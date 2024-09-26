#!/usr/bin/env bash

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile.properties.gpg
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output snowhouse.properties scripts/snowhouse.properties.gpg

sbt clean compile
sbt "testOnly com.snowflake.snowpark.PerfTest" -J-DargLine="-DPERF_TEST=true -Xss1G"

