#!/bin/bash -ex

# verify only one argument is provided
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <SCALA_VERSION>" >&2
    exit 1
fi

# validate argument correctness
case "$1" in
    "2.12.20"|"2.13.16")
        # Valid option
        ;;
    *)
        echo "Error: Invalid option. Expected '2.12.20', or '2.13.16'." >&2
        exit 1
        ;;
esac

# format src
sbt clean
sbt "++ $1 compile"

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
  echo "Run 'sbt +test:javafmt' to reformat"
  exit 1
fi
