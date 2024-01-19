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
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile.properties.gpg

exit_code_decorator "mvn clean compile"
exit_code_decorator "mvn -Dgpg.skip -DtagsToExclude=com.snowflake.snowpark.PerfTest -e scoverage:report -P test-coverage"
