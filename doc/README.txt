Snowpark (Preview Version)
--------------------------

Snowpark is a client library for querying and interacting with the Snowflake database.

This distribution of the Snowpark library for Scala and Java includes the following files and
directories:

- snowpark-x.x.x.jar contains the classes for the library.
- The lib directory contains the dependencies for the Snowpark library.
- snowpark-x.x.x-with-dependencies.jar contains the classes for the library and the dependencies.
- The docs directory contains the scaladoc and javadoc for the Snowpark API.
- run.sh starts the Scala REPL with the Snowpark library dependencies in the classpath.
- preload.scala is used by the run.sh script to import the com.snowflake.snowpark package and the
  com.snowflake.snowpark.functions object.
