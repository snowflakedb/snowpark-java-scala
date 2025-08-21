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
