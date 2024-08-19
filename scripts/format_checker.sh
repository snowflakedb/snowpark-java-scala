#!/bin/bash -ex

sbt clean compile

if [ -z "$(git status --porcelain)" ]; then
  echo "Code Format Check: Passed!"
else
  echo "Code Format Check: Failed!"
  echo "Run 'sbt clean compile' to reformat"
  exit 1
fi
