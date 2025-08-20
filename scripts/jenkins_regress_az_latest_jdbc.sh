#!/usr/bin/env bash

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

exit_code_decorator(){
  cmd=$1
    args=${@:2}
    echo $1
    $cmd $args

    if [ $? -ne 0 ]; then
    	echo "Command '${1}' FAILED"
   		exit 1
	fi
}

# test
# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile_az.properties.gpg

build_jdbc

# skip com.snowflake.snowpark.ReplSuite because classpath are not set well for local jdbc jar
rm -fr ./src/test/scala/com/snowflake/snowpark/ReplSuite.scala

exit_code_decorator "sbt clean +compile"
exit_code_decorator "sbt +JavaAPITests:test"
exit_code_decorator "sbt +NonparallelTests:test"
exit_code_decorator "sbt \"UDFTests:testOnly * -- -l SampleDataTest\""
exit_code_decorator "sbt \"++ 2.13.16 UDFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest -l SampleDataTest\""
exit_code_decorator "sbt UDTFTests:test"
exit_code_decorator "sbt \"++ 2.13.16 UDTFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest\""
exit_code_decorator "sbt +SprocTests:test"
exit_code_decorator "sbt +OtherTests:test"

# clean up
rm -fr snowflake-jdbc
git checkout -- .
