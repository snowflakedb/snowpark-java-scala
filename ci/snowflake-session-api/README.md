# snowflake-session-api (vendored sources)

**Temporary. Delete this directory and the `sessionApiSources` lines in `build.sbt` once
`com.snowflake:snowflake-session-api` is published; then depend on the published artifact instead.**

The driver-neutral session interface (`com.snowflake.session.api`) is not yet published to any
repository. Until it is, `build.sbt` compiles the sources in this directory
(`src/main/java/com/snowflake/session/api/`) into the Snowpark jar. This means:

- Building needs nothing but `sbt`; there is no separate install step.
- The published Snowpark POM has no dependency on `snowflake-session-api`, so it resolves from
  Maven Central.
- The classes are part of the Snowpark jar (and the `with-dependencies` jar), so they travel with
  Snowpark when it is uploaded for UDFs and stored procedures. They are excluded from the
  Scaladoc and Javadoc.
- The Snowflake JDBC driver declares `snowflake-session-api` as `provided`. Its session factory
  implements the interface bundled here, so the interface classes must come from the Snowpark jar
  and be loaded by the same class loader as the driver. Do not put a second copy on the classpath.

The sources under `src/main/java/com/snowflake/session/api/` are temporary, verbatim copies of the
unpublished session API. Do not edit them independently; replace them with the published artifact
when it is available.
