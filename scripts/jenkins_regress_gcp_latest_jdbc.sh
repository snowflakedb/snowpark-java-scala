#!/usr/bin/env bash
set -Eeuxo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

build_jdbc() {
  git clone git@github.com:snowflakedb/snowflake-jdbc.git
  cd snowflake-jdbc
  echo "building jdbc ..."
  mvn clean install -D skipTests=true >& jdbc.build.out
  tail jdbc.build.out
  cd ..
  ls snowflake-jdbc/target/snowflake-jdbc*.jar
  # snowflake-jdbc/target/snowflake-jdbc.jar is the compiled jar file
  # The sed command on mac is different, below is the command on mac
  if [ "$(uname)" == "Darwin" ]; then
    sed -i '' 's#<version>\${snowflake.jdbc.version}</version>#<version>\${snowflake.jdbc.version}</version>\n            <scope>system</scope>\n            <systemPath>'$PWD'/snowflake-jdbc/target/snowflake-jdbc.jar</systemPath>#g' pom.xml
  else
    sed -i 's#<version>\${snowflake.jdbc.version}</version>#<version>\${snowflake.jdbc.version}</version>\n            <scope>system</scope>\n            <systemPath>'$PWD'/snowflake-jdbc/target/snowflake-jdbc.jar</systemPath>#g' pom.xml
  fi
  echo "After replace snowflake jdbc jar with sed"
  git diff pom.xml
}

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile_gcp.properties.gpg

build_jdbc

# skip com.snowflake.snowpark.ReplSuite because classpath are not set well for local jdbc jar
rm -fr ./src/test/scala/com/snowflake/snowpark/ReplSuite.scala

# test
sbt clean +compile
sbt +JavaAPITests:test
sbt +NonparallelTests:test
sbt "UDFTests:testOnly * -- -l SampleDataTest"
sbt "++ 2.13.16 UDFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest -l SampleDataTest"
sbt UDTFTests:test
sbt "++ 2.13.16 UDTFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest"
sbt +SprocTests:test
sbt +OtherTests:test

# clean up
cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  rm -fr snowflake-jdbc
  git checkout -- .
}
