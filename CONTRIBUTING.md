# Guide for Contibutors

Are you an external contibutor? Follow this guide to make your first pull request!

## Environment Setup

### Prerequisites

- Java 11
- Maven 3
- A Snowflake account

### Setup instructions

1. Create a fork of this repository into your personal account and clone the repo locally. 
2. Create `profile.properties` from the template (`profile.properties.example`).
3. Run the test suite using the following command

    ```bash 
    mvn -DtagsToExclude=com.snowflake.snowpark.UDFTest -Dgpg.skip scoverage:report
    ```
    
If the tests execute successfully, you can proceed to making your contribution/addition. When you're ready, head to the next section to create a pull request.

### Code Overview

- [scala/](src/main/scala/) is the primary implentation of the SDK
- [java/](src/main/java/) is a wrapper around the Scala implementation for the Java SDK

## Create a PR

Before opening a pull request on the upstream repo, we recommend running the following tasks locally.

### Run local tests and formatter

Run the following command to execute the test suite and and the formatter.

```bash
mvn clean compile
```

### Generate the Java and Scala docs

The previous command will generate the Scala documentation. Run this command to generate the docs for the Java API:

```bash
scripts/generateJavaDoc.sh
```

### Open your pull request

After you have run the test suite, formatter, and docs locally open a pull request on this repo and add @sfc-gh-bli as a reviewer. We will run additional tests against your pull request.

## Appendix

### Build Snowpark JAR

To compile the latest code and build a jar without running tests, use the command:

```bash 
mvn -DskipTests -Dgpg.skip package
```

This target also copies all the dependencies to the folder target/dependency.

### Generate Scala API Docs

Quick test:

```bash
mvn scala:doc
```

Apply All Configurations:

```bash
mvn -DskipTests -Dgpg.skip package
```

Doc can be found in `snowpark-java-scala/target/site`

### Generate Java API Docs

```bash
scripts/generateJavaDoc.sh
```

Doc can be found in `snowpark-java-scala/javaDoc/target/site`

### Add New Java Test Suite

Snowpark triggers all Java tests from Scala test suite `JavaAPISuite`. 
So everytime when adding a new Java test suite, please update `JavaAPISuite` to trigger it,
otherwise, that new Java test suite will be ignored.

### Upload Test Coverage Report to SonarQube

**Note:** This section is only applicable to Snowflake employees.

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
