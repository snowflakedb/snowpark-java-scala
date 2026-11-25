# Guide for Contibutors

Are you an external contibutor? Follow this guide to make your first pull request!

## Environment Setup

### Prerequisites

- Java 11
- SBT
- A Snowflake account

### Setup instructions

1. Create a fork of this repository into your personal account and clone the repo locally.
2. Create `profile.properties` from the template (`profile.properties.example`).
3. Run the test suite using the following command

    ```bash 
    sbt +test
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
sbt clean +compile +test
```

### Generate the Scala docs

Run this command to generate the docs for the Scala APIs (for both 2.12, and 2.13):

```bash
sbt +doc
```

### Generate the Java docs

The previous command will generate the Scala documentation. Run this command to generate the docs for the Java API:

```bash
sbt +genjavadoc:doc
```

### Open your pull request

After you have run the test suite, formatter, and docs locally open a pull request on this repo and add @sfc-gh-bli as a reviewer. We will run additional tests against your pull request.

## Appendix

### Build Snowpark JAR

To compile the latest code and build a jar without running tests, use the command:

```bash 
sbt +package
```

### Add New Java Test Suite

Snowpark triggers all Java tests from Scala test suite `JavaAPISuite`. 
So everytime when adding a new Java test suite, please update `JavaAPISuite` to trigger it,
otherwise, that new Java test suite will be ignored.

### JDBC driver versions

`jdbcVersionExact` in `build.sbt` is the only hand-edited production JDBC version. It is the JDBC 4.x
driver in the published POM, in the client distribution, and for the Java stored-procedure exact
runtime. Snowpark finds the driver's `SnowflakeClientSessionFactory` through `ServiceLoader`.
JDBC 3.27.1 (`jdbcVersion`) is kept only to compile and test the legacy `Jdbc3Session` fallback; it
lives in the hidden `jdbc3-legacy` configuration and is never published or placed on a production
classpath. Fat jars never contain a JDBC driver.

- `sbt jdbc4Compat/test` runs Snowpark on a classpath with only the exact driver, and checks the
  published POM and the packaged `META-INF/snowflake/snowpark-exact-runtime.properties`.
- `scripts/exact-runtime-metadata.sh [OUTPUT_FILE]` (or `sbt printExactRuntimeMetadata`) prints
  `JDBC_GAV` and `JDBC_SHA256` for adapter and RPM automation.
- To validate an unreleased driver locally (local-only; never for a release), do **not** install it
  into `~/.m2`: a locally built snapshot there has the JDBC *build* POM, which pulls in hundreds of
  internal transitive jars and changes Snowpark's own dependency versions, so the result is not
  representative. Instead put the jar and JDBC's `public_pom.xml` (`FIPS/public_pom.xml` for FIPS,
  with `--fips`) into an isolated repository of your choice, then run sbt against it:

  ```bash
  scripts/install-local-jdbc.sh --repo "$REPO" --version 4.4.2-SNAPSHOT \
    --jar /path/to/snowflake-jdbc.jar --pom /path/to/snowflake-jdbc/public_pom.xml
  sbt clean
  sbt -Dsnowpark.jdbcVersionExact=4.4.2-SNAPSHOT -Dmaven.repo.local="$REPO" \
    "++ 2.12.20 jdbc4Compat/test" "++ 2.13.16 jdbc4Compat/test"
  ```

  Run `sbt clean` whenever `snowpark.jdbcVersionExact` or the repository changes; otherwise sbt can
  reuse a stale dependency resolution. The override requires an explicit `-Dmaven.repo.local` (the
  build rejects `~/.m2/repository`), does not change the default in `build.sbt`, and publishing with
  it set fails.
