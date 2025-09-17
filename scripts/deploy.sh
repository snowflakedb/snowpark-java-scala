#!/bin/bash -ex
#
# Push Snowpark Java/Scala build to the public maven repository.
# This script needs to be executed by snowflake jenkins job.
#

unset SNOWPARK_FIPS
source scripts/deploy-common.sh
