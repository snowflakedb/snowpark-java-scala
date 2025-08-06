#!/bin/bash -ex

# format src
sbt clean +compile

if [ -z "$(git status --porcelain)" ]; then
  echo "Code Format Check: Passed!"
else
  echo "Code Format Check: Failed!"
  echo "Run 'sbt clean +compile' to reformat"
  exit 1
fi

# format scala test
sbt +test:scalafmt
if [ -z "$(git status --porcelain)" ]; then
  echo "Scala Test Code Format Check: Passed!"
else
  echo "Scala Test Code Format Check: Failed!"
  echo "Run 'sbt +test:scalafmt' to reformat"
  exit 1
fi

# format java test
sbt +test:javafmt
if [ -z "$(git status --porcelain)" ]; then
  echo "Scala Test Code Format Check: Passed!"
else
  echo "Scala Test Code Format Check: Failed!"
  echo "Run 'sbt +test:scalafmt' to reformat"
  exit 1
fi
