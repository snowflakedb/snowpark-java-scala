#!/bin/bash -ex

mvn clean compile

if [ -z "$(git status --porcelain)" ]; then
  echo "Code Format Check: Passed!"
else
  echo "Code Format Check: Failed!"
  echo "Run 'mvn clean compile' to reformat"
  exit 1
fi
