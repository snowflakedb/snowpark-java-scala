#!/usr/bin/env bash
source scripts/utils.sh
decrypt_profile_properties_gpg 'scripts/profile_az.properties.gpg'
run_test_suites
