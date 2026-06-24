#!/usr/bin/env bash
set -euxo pipefail

# decrypt profile
# disable xtrace so GPG_KEY passphrase is not echoed to logs
set +x
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile.properties.gpg
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output snowhouse.properties scripts/snowhouse.properties.gpg
# re-enable xtrace now that the passphrase is no longer referenced
set -x

sbt clean +compile
sbt -J-DargLine="-DPERF_TEST=true -Xss1G" "+testOnly com.snowflake.snowpark.PerfTest"
export JAVA_OPTS="-Xss1G"
sbt -J-DPERF_TEST=true "+testOnly com.snowflake.perf.OptimizerPerfSuite"

