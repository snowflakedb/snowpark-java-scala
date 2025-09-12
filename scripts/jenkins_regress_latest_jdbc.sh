#!/usr/bin/env bash
source scripts/utils.sh
build_and_use_latest_jdbc
decrypt_profile_properties_gpg 'scripts/profile.properties.gpg'
run_test_suites
