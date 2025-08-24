#!/usr/bin/env bash
set -euxo pipefail

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile_gcp.properties.gpg

# test
sbt clean +compile
sbt +JavaAPITests:test
sbt +NonparallelTests:test
sbt "UDFTests:testOnly * -- -l SampleDataTest"
sbt "++ 2.13.16 UDFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest -l SampleDataTest"
sbt UDTFTests:test
sbt "++ 2.13.16 UDTFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest"
sbt +SprocTests:test
sbt +OtherTests:test
