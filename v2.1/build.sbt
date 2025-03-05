lazy val `macro` = (project in file("macro"))
  .settings(
    name := "snowpark-macro",
    scalaVersion := "2.13.16",
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  )

lazy val lib = (project in file("lib"))
  .settings(name := "snowpark-lib", scalaVersion := "2.13.16")
  .dependsOn(`macro`)

lazy val root = (project in file("."))
  .aggregate(`macro`, lib)
  .dependsOn(`macro`, lib)
  .settings(name := "snowpark")
