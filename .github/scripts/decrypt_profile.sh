#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PROFILE_PASSWORD" --output profile.properties profile.properties.gpg
