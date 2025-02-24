lazy val snowparkName = s"snowpark"

lazy val root = (project in file("."))
  .settings(
    name := snowparkName,
    version := "2.0.0-SNAPSHOT",
    scalaVersion := sys.props.getOrElse("SCALA_VERSION", default = "2.12.18"),
    crossScalaVersions := Seq("2.12.18", "2.13.15"),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      // Tests
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
      "org.mockito" % "mockito-core" % "2.23.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    )
  )
