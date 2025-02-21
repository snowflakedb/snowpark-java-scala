lazy val snowparkName = s"snowpark"

lazy val root = (project in file("."))
  .settings(
    name := snowparkName,
    version := "2.0.0-SNAPSHOT",
    scalaVersion := sys.props.getOrElse("SCALA_VERSION", default = "2.12.18"),
    crossScalaVersions := Seq("2.12.18", "2.13.15"),
    libraryDependencies ++=Seq(
      "net.snowflake" % "snowflake-jdbc" % "3.22.0",
    )
  )
