addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")

// fix scala style plugin depends old scala-xml lib issue
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" %
  VersionScheme.Always