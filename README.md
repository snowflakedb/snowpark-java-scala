# Snowpark

![Build Status](https://github.com/snowflakedb/thundersnow/workflows/precommit%20test/badge.svg)

## To build Snowpark jar
To compile the latest code and build a jar without running tests, use the command:

```bash 
mvn -DskipTests -Dgpg.skip package
```
This target also copies all the dependencies to the folder target/dependency.

## Generate API Doc
### Scala Doc
#### Quick Test
```bash
mvn scala:doc
```

#### Apply All Configurations
```bash
mvn -DskipTests -Dgpg.skip package
```

Doc can be found in `thundersnow/target/site`

### Java Doc

```bash
scripts/generateJavaDoc.sh
```

Doc can be found in `thundersnow/javaDoc/target/site`

## Setup Dev Env

1. Clone this repo.

2. Create `profile.properties` from the template (`profile.properties.example`).

3. Run test by 

```bash 
mvn -DtagsToExclude=com.snowflake.snowpark.UDFTest -Dgpg.skip scoverage:report
```

Test coverage report can be found in `target/site/scoverage/index.html`

## Run Formatter

```bash
mvn clean compile
```

## Setup Pre-Commit Hook

[Instruction](https://snowflakecomputing.atlassian.net/wiki/spaces/CLO/pages/1212940297/Pre-Commit+Secret+Scanning+Instructions#How-to-Enable-Pre-Commit-Secret-Scanning-Locally)

## Add New Java Test Suite

Snowpark triggers all Java tests from Scala test suite `JavaAPISuite`. 
So everytime when adding a new Java test suite, please update `JavaAPISuite` to trigger it,
otherwise, that new Java test suite will be ignored.

## Upload Test Coverage Report to SonarQube

### Prerequisite
Install sonar-scanner

https://docs.sonarqube.org/latest/analysis/scan/sonarscanner/

### Script

```bash
# clean repo
mvn clean verify -DskipTests -Dgpg.skip

# generate report
mvn -Dgpg.skip -e scoverage:report

# setup version
version=`git rev-parse HEAD`

snowpark_home= # path to snowpark repo 

sonar-scanner \
    -Dsonar.host.url=https://sonarqube.int.snowflakecomputing.com \
    -Dsonar.projectBaseDir=${snowpark_home} \
    -Dsonar.projectVersion="$version" \
    -Dsonar.scala.coverage.reportPaths=target/scoverage.xml \
    -Dsonar.sources=src/main \
    -Dsonar.binaries=target/scoverage-classes \
    -Dsonar.java.binaries=target/scoverage-classes \
    -Dsonar.tests=src/test \
    -Dsonar.scala.version=2.12 \
    -Dsonar.projectKey=snowpark \
    -Dsonar.scm.revision=${version} \
    -Dsonar.scm.provider=git
```

### Test Report

[Test Coverage](https://sonarqube.int.snowflakecomputing.com/dashboard?id=snowpark)