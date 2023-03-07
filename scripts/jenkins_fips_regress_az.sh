#!/usr/bin/env bash


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

exit_code_decorator "mvn clean compile"
exit_code_decorator "mvn -f fips-pom.xml -DargLine=-DFIPS_TEST=true -Dgpg.skip -DtagsToExclude=UnstableTest,com.snowflake.snowpark.PerfTest,SampleDataTest test"

