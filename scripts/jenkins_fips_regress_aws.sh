#!/usr/bin/env bash
source scripts/utils.sh
decrypt_profile_properties_gpg 'scripts/profile.properties.gpg'
export SNOWPARK_FIPS="true"
run_test_suites
