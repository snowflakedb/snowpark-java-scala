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
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output profile.properties scripts/profile_gcp.properties.gpg

exit_code_decorator "sbt clean +compile"
exit_code_decorator "sbt +JavaAPITests:test"
exit_code_decorator "sbt +NonparallelTests:test"
exit_code_decorator "sbt UDFTests:testOnly * -- -l SampleDataTest"
exit_code_decorator "sbt \"++ 2.13.16 UDFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest -l SampleDataTest\""
exit_code_decorator "sbt UDTFTests:test"
exit_code_decorator "sbt \"++ 2.13.16 UDTFTests:testOnly * -- -l com.snowflake.snowpark.UDFPackageTest\""
exit_code_decorator "sbt +SprocTests:test"
exit_code_decorator "sbt +OtherTests:test"
