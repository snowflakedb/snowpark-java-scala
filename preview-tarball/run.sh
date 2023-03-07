#!/usr/bin/env bash

# verify scala
if ! command -v scala &> /dev/null
then
    echo "Scala REPL could not be found."
    exit
fi

# Enter directory of run.sh no matter where the script is called
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
pushd "$parent_path"

# Find the latest snowpark jar file
snowpark_jar=$( ls -l snowpark*.jar | awk '{print $9}' | tail -1 )

scala -cp "$snowpark_jar:lib/*" -Yrepl-class-based -Yrepl-outdir repl_classes -i preload.scala

popd

