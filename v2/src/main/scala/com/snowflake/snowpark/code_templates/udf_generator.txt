(0 to 2).map { x =>
  val types = (1 to x).foldRight("RT")((i, s) => s"A$i, $s")
  val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
  val s = if (x > 1) "s" else ""
  val version = if (x > 10) "0.12.0" else "0.1.0"
  s"""  /**
     |   * Registers a Scala closure of $x argument$s as a Snowflake Java UDF and returns the UDF.
     |   * @tparam RT return type of UDF.
     |   * @group udf_func
     |   * @since $version
     |   */
     |  def udf$x(): Unit = {}""".stripMargin
}.mkString("\n")
