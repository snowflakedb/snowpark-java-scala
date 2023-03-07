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
exit_code_decorator "mvn -Dgpg.skip -DtagsToExclude=com.snowflake.snowpark.PerfTest -e scoverage:report"

version=`git rev-parse HEAD`

if [ -z $pr_code_coverage ];
 then
        echo "For master"
	/usr/local/sonar-scanner-cli/bin/sonar-scanner \
    	-Dsonar.host.url=https://sonarqube.int.snowflakecomputing.com \
    	-Dsonar.projectBaseDir=/mnt/jenkins${WORKSPACE}/thundersnow \
    	-Dsonar.projectVersion=1.8.0 \
    	-Dsonar.scala.coverage.reportPaths=target/scoverage.xml \
    	-Dsonar.coverage.jacoco.xmlReportPaths=target/site/jacoco/jacoco.xml \
    	-Dsonar.sources=src/main \
    	-Dsonar.binaries=target/scoverage-classes \
    	-Dsonar.java.binaries=target/scoverage-classes \
    	-Dsonar.tests=src/test \
    	-Dsonar.scala.version=2.12 \
    	-Dsonar.projectKey=snowpark \
    	-Dsonar.scm.revision=${version} \
    	-Dsonar.scm.provider=git
else
        echo "For Pull Request"
	/usr/local/sonar-scanner-cli/bin/sonar-scanner \
        -Dsonar.host.url=https://sonarqube.int.snowflakecomputing.com \
        -Dsonar.projectBaseDir=/mnt/jenkins${WORKSPACE}/thundersnow \
        -Dsonar.projectVersion=1.8.0 \
        -Dsonar.scala.coverage.reportPaths=target/scoverage.xml \
        -Dsonar.coverage.jacoco.xmlReportPaths=target/site/jacoco/jacoco.xml \
        -Dsonar.sources=src/main \
        -Dsonar.binaries=target/scoverage-classes \
        -Dsonar.java.binaries=target/scoverage-classes \
        -Dsonar.tests=src/test \
        -Dsonar.scala.version=2.12 \
        -Dsonar.projectKey=snowpark \
        -Dsonar.scm.revision=${version} \
        -Dsonar.scm.provider=git \
        -Dsonar.pullrequest.key=${PR_KEY} \
        -Dsonar.pullrequest.branch=${PR_ID} \
        -Dsonar.pullrequest.base=master
fi
