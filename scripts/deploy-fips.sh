#!/bin/bash -ex
#
# Push Snowpark Java/Scala FIPS build to the public maven repository.
# This script needs to be executed by snowflake jenkins job.
#

export SNOWPARK_FIPS="true"
source scripts/deploy-common.sh
