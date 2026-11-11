package com.snowflake.snowpark.internal.jdbc3

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException, Statement}

import com.snowflake.session.api.{FieldMetadata, SnowflakeSession, SnowflakeSessionLoader}
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.internal.{ServerConnection, SnowflakeResultSetExt}
import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.core.{SFBaseResultSet, SFException, SFSessionProperty}
import net.snowflake.client.jdbc.{
  ErrorCode,
  SnowflakeBaseResultSet,
  SnowflakeConnectionV1,
  SnowflakeReauthenticationRequest,
  SnowflakeSQLException
}
import net.snowflake.client.core.arrow.StructObjectWrapper
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.JsonStringArrayList
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

// Runs with the default JDBC 3.x dependency, i.e. the client and legacy stored-procedure path.
class Jdbc3SessionSuite extends AnyFunSuite {

  private def storedProcSession() = {
    val conn = Mockito.mock(classOf[SnowflakeConnectionV1])
    (Jdbc3Session.forStoredProcedure(conn), conn)
  }

  test("JDBC 3.x provides no client session factory, so Snowpark uses its own") {
    assert(!SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader).isPresent)
  }

  test("legacy stored-procedure session reports itself as a stored procedure") {
    val (session, conn) = storedProcSession()
    assert(session.isStoredProcedure)
    assert(session.getConnection eq conn)
  }

  test("child query IDs come from the connection") {
    val (session, conn) = storedProcSession()
    Mockito.when(conn.getChildQueryIds("q")).thenReturn(Array("q1", "q2"))
    assert(session.getChildQueryIds("q") == java.util.Arrays.asList("q1", "q2"))
  }

  test("Snowflake errors are the driver's SnowflakeSQLException only") {
    val (session, _) = storedProcSession()
    assert(session.isSnowflakeError(new SnowflakeSQLException("invalid identifier 'A'", "42000")))
    assert(!session.isSnowflakeError(new SQLException("plain")))
    assert(!session.isSnowflakeError(new RuntimeException("other")))
  }

  test("reauthentication requests are recognized") {
    val (session, _) = storedProcSession()
    assert(
      session.isReauthenticationRequest(
        new SnowflakeReauthenticationRequest("qid", "expired", "08001", 390112)))
    assert(!session.isReauthenticationRequest(new SnowflakeSQLException("x", "08001")))
  }

  test("driver-internal errors have the internal SQL state and no query") {
    val (session, _) = storedProcSession()
    assert(session.isDriverInternalError(new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "x")))
    assert(
      !session.isDriverInternalError(new SnowflakeSQLException("qid", "server error", "XX000")))
    assert(!session.isDriverInternalError(new SnowflakeSQLException("syntax error", "42000")))
    assert(session.isDriverInternalError(new SFException(ErrorCode.INTERNAL_ERROR, "x")))
    assert(!session.isDriverInternalError(new RuntimeException("user")))
  }

  test("stored-procedure entry point rejects null and client sessions") {
    intercept[IllegalArgumentException](StoredProcSessionFactory.fromSnowflakeSession(null))
    val client = Mockito.mock(classOf[com.snowflake.session.api.SnowflakeSession])
    Mockito.when(client.isStoredProcedure).thenReturn(false)
    val ex =
      intercept[IllegalArgumentException](StoredProcSessionFactory.fromSnowflakeSession(client))
    assert(ex.getMessage.contains("is not a stored-procedure session"))
  }

  test("client info property key agrees with JDBC 3.x") {
    assert(SFSessionProperty.CLIENT_INFO.getPropertyKey == "snowflakeClientInfo")
  }

  // ---- structured ARRAY values ----

  private def field(
      typeName: String,
      sqlType: Int,
      name: String = "",
      fields: Seq[FieldMetadata] = Nil): FieldMetadata =
    FieldMetadata.builder(typeName, sqlType).name(name).fields(fields.asJava).build()

  private def numberField(): FieldMetadata =
    FieldMetadata.builder("NUMBER", java.sql.Types.INTEGER).name("").precision(10).build()

  private def arrayField(element: FieldMetadata = numberField()): FieldMetadata =
    field("ARRAY", java.sql.Types.ARRAY, fields = Seq(element))

  // a SnowflakeBaseResultSet whose underlying sfBaseResultSet returns `raw` for column 1
  private def resultSetReturning(raw: AnyRef): ResultSet = {
    val sfBase = Mockito.mock(classOf[SFBaseResultSet])
    Mockito.when(sfBase.getObject(1)).thenReturn(raw, Nil: _*)
    val rs = Mockito.mock(classOf[SnowflakeBaseResultSet])
    val field = classOf[SnowflakeBaseResultSet].getDeclaredField("sfBaseResultSet")
    field.setAccessible(true)
    field.set(rs, sfBase)
    rs
  }

  private def decodeArray(raw: AnyRef): AnyRef =
    new Jdbc3StructuredDecoder(resultSetReturning(raw)).decode(1, arrayField())

  private def numberValue(i: Int): AnyRef = java.math.BigDecimal.valueOf(i.toLong)

  private def list(values: AnyRef*): java.util.List[AnyRef] =
    new java.util.ArrayList[AnyRef](values.asJava)

  // converts `decoded` as the value of a column of type `column`
  private def convert(decoded: AnyRef, column: FieldMetadata, legacyArrays: Boolean): Any = {
    val rs = Mockito.mock(classOf[ResultSet])
    val meta = Mockito.mock(classOf[ResultSetMetaData])
    Mockito.when(rs.getMetaData).thenReturn(meta)
    Mockito.when(meta.getColumnName(1)).thenReturn("c")
    Mockito.when(meta.getColumnTypeName(1)).thenReturn(column.getTypeName)
    Mockito.when(meta.getColumnType(1)).thenReturn(column.getSqlType)
    val session = Mockito.mock(classOf[SnowflakeSession])
    Mockito.when(session.getColumnFields(meta, 1)).thenReturn(column.getFields)
    Mockito.when(session.getStructuredObject(rs, 1)).thenReturn(decoded, Nil: _*)
    new SnowflakeResultSetExt(rs, session, legacyArrays).getObject(1)
  }

  test("decoder returns a java.util.List for every driver representation of an array") {
    val arrayList = new java.util.ArrayList[AnyRef]()
    arrayList.add(numberValue(1))
    arrayList.add(null)
    val jsonList = new JsonStringArrayList[AnyRef]()
    jsonList.add(numberValue(1))
    jsonList.add(null)
    val wrapped = new JsonStringArrayList[AnyRef]()
    wrapped.add(numberValue(1))
    wrapped.add(null)
    Seq(arrayList, jsonList, new StructObjectWrapper(null, wrapped)).foreach { raw =>
      val decoded = decodeArray(raw)
      assert(decoded.getClass == classOf[java.util.ArrayList[_]])
      assert(decoded == java.util.Arrays.asList(numberValue(1), null))
    }
  }

  test("top-level array is a Buffer, or an Array with JDBC older than 3.21") {
    val converted = convert(list(numberValue(1), null), arrayField(), legacyArrays = false)
    assert(converted.isInstanceOf[scala.collection.mutable.Buffer[_]])
    assert(
      converted.asInstanceOf[scala.collection.mutable.Buffer[Any]].toSeq == Seq(
        numberValue(1),
        null))
    val legacy = convert(list(numberValue(1), null), arrayField(), legacyArrays = true)
    assert(legacy.isInstanceOf[Array[_]])
    assert(legacy.asInstanceOf[Array[Any]].toSeq == Seq(numberValue(1), null))
  }

  test("nested arrays are always Arrays") {
    Seq(true, false).foreach { legacyArrays =>
      val inArray =
        convert(list(list(numberValue(1)), list()), arrayField(arrayField()), legacyArrays)
      val outer = inArray match {
        case top: Array[_] => top.toSeq
        case top: scala.collection.mutable.Buffer[_] => top.toSeq
      }
      assert(outer.forall(_.isInstanceOf[Array[_]]))
      assert(outer.map(_.asInstanceOf[Array[Any]].toSeq) == Seq(Seq(numberValue(1)), Seq()))

      val mapField = field(
        "OBJECT",
        java.sql.Types.STRUCT,
        fields = Seq(field("VARCHAR", java.sql.Types.VARCHAR), arrayField()))
      val map = new java.util.LinkedHashMap[AnyRef, AnyRef]()
      map.put("k", list(numberValue(2)))
      val inMap = convert(map, mapField, legacyArrays).asInstanceOf[Map[Any, Any]]
      assert(inMap("k").asInstanceOf[Array[Any]].toSeq == Seq(numberValue(2)))

      val objectField = field(
        "OBJECT",
        java.sql.Types.STRUCT,
        fields = Seq(field("ARRAY", java.sql.Types.ARRAY, "a", Seq(numberField()))))
      val obj = new java.util.LinkedHashMap[String, AnyRef]()
      obj.put("a", list(numberValue(3)))
      val inObject = convert(obj, objectField, legacyArrays).asInstanceOf[Row]
      assert(inObject.get(0).asInstanceOf[Array[Any]].toSeq == Seq(numberValue(3)))
    }
  }

  test("nested structured maps keep the driver's entry order") {
    val mapField = field(
      "OBJECT",
      java.sql.Types.STRUCT,
      fields = Seq(field("VARCHAR", java.sql.Types.VARCHAR), numberField()))
    val map = new java.util.LinkedHashMap[AnyRef, AnyRef]()
    map.put("a", numberValue(1))
    map.put("b", numberValue(2))
    val inArray = convert(list(map), arrayField(mapField), legacyArrays = false)
      .asInstanceOf[scala.collection.mutable.Buffer[Any]]
    assert(inArray.head.asInstanceOf[Map[Any, Any]].keys.toSeq == Seq("a", "b"))
  }

  // reads the single top-level ARRAY column of a one-row result through `server`
  private def readArrayColumn(server: ServerConnection, session: SnowflakeSession): Any = {
    val rs = Mockito.mock(classOf[ResultSet])
    val meta = Mockito.mock(classOf[ResultSetMetaData])
    Mockito.when(rs.getMetaData).thenReturn(meta)
    Mockito.when(meta.getColumnCount).thenReturn(1)
    Mockito.when(meta.getColumnLabel(1)).thenReturn("C0")
    Mockito.when(meta.getColumnName(1)).thenReturn("C0")
    Mockito.when(meta.getColumnTypeName(1)).thenReturn("ARRAY")
    Mockito.when(meta.getColumnType(1)).thenReturn(java.sql.Types.ARRAY)
    Mockito.when(session.getColumnFields(any(), anyInt())).thenReturn(arrayField().getFields)
    Mockito.when(session.getStructuredObject(rs, 1)).thenReturn(list(numberValue(1)), Nil: _*)
    Mockito.when(rs.next()).thenReturn(true, false)
    val statement = Mockito.mock(classOf[Statement])
    Mockito.when(statement.getResultSet).thenReturn(rs)
    server.resultSetToIterator(statement)._1.next().get(0)
  }

  private def serverOnDriver(major: Int, minor: Int) = {
    val session = Mockito.mock(classOf[SnowflakeSession])
    val conn = Mockito.mock(classOf[Connection])
    val dbMeta = Mockito.mock(classOf[java.sql.DatabaseMetaData])
    Mockito.when(conn.isClosed).thenReturn(false)
    Mockito.when(conn.getMetaData).thenReturn(dbMeta)
    Mockito.when(dbMeta.getDriverMajorVersion).thenReturn(major)
    Mockito.when(dbMeta.getDriverMinorVersion).thenReturn(minor)
    Mockito.when(session.getConnection).thenReturn(conn)
    (new ServerConnection(Map.empty, true, Some(session)), session, conn)
  }

  test("ServerConnection picks Array or Buffer from the JDBC driver version") {
    Seq((2, 99) -> true, (3, 20) -> true, (3, 21) -> false, (4, 0) -> false).foreach {
      case ((major, minor), legacy) =>
        val (server, session, _) = serverOnDriver(major, minor)
        val value = readArrayColumn(server, session)
        assert(value.isInstanceOf[Array[_]] == legacy, s"driver $major.$minor")
        assert(value.isInstanceOf[scala.collection.mutable.Buffer[_]] == !legacy)
    }
  }

  test("ServerConnection reads the driver version once") {
    val (server, session, conn) = serverOnDriver(3, 21)
    readArrayColumn(server, session)
    readArrayColumn(server, session)
    Mockito.verify(conn, Mockito.times(1)).getMetaData
  }

  // ---- null cells are detected before the typed getter is read ----

  test("row iterator returns null for null cells without consulting the typed getter") {
    val types = Seq(
      ("INTEGER", java.sql.Types.INTEGER),
      ("BOOLEAN", java.sql.Types.BOOLEAN),
      ("DOUBLE", java.sql.Types.DOUBLE),
      ("VARCHAR", java.sql.Types.VARCHAR))
    val session = Mockito.mock(classOf[SnowflakeSession])
    val conn = Mockito.mock(classOf[Connection])
    Mockito.when(conn.isClosed).thenReturn(false)
    Mockito.when(session.getConnection).thenReturn(conn)
    val rs = Mockito.mock(classOf[ResultSet])
    val meta = Mockito.mock(classOf[ResultSetMetaData])
    Mockito.when(rs.getMetaData).thenReturn(meta)
    Mockito.when(meta.getColumnCount).thenReturn(types.size)
    types.zipWithIndex.foreach { case ((name, sqlType), i) =>
      Mockito.when(meta.getColumnLabel(i + 1)).thenReturn(s"C$i")
      Mockito.when(meta.getColumnTypeName(i + 1)).thenReturn(name)
      Mockito.when(meta.getColumnType(i + 1)).thenReturn(sqlType)
      Mockito.when(meta.isSigned(i + 1)).thenReturn(true)
      Mockito.when(meta.isNullable(i + 1)).thenReturn(ResultSetMetaData.columnNullable)
    }
    Mockito
      .when(session.getColumnFields(any(), anyInt()))
      .thenReturn(java.util.Collections.emptyList[FieldMetadata]())
    // first row: all null; the typed getters would return defaults (0 / false) if consulted
    Mockito.when(rs.next()).thenReturn(true, false)
    Mockito.when(rs.getObject(anyInt())).thenReturn(null, Nil: _*)
    Mockito.when(rs.getInt(anyInt())).thenReturn(0)
    Mockito.when(rs.getBoolean(anyInt())).thenReturn(false)
    Mockito.when(rs.getDouble(anyInt())).thenReturn(0.0)
    val statement = Mockito.mock(classOf[Statement])
    Mockito.when(statement.getResultSet).thenReturn(rs)

    val server = new ServerConnection(Map.empty, true, Some(session))
    val (iterator, _) = server.resultSetToIterator(statement)
    assert(iterator.hasNext)
    val row = iterator.next()
    assert(row == Row(null, null, null, null))
    (1 to types.size).foreach(i => assert(row.isNullAt(i - 1)))
    Mockito.verify(rs, Mockito.never()).getInt(anyInt())
    Mockito.verify(rs, Mockito.never()).getBoolean(anyInt())
    Mockito.verify(rs, Mockito.never()).getDouble(anyInt())
    Mockito.verify(rs, Mockito.never()).getString(anyInt())
    Mockito.verify(rs, Mockito.never()).wasNull()
  }

  test("row iterator reads the typed getter for non-null cells") {
    val session = Mockito.mock(classOf[SnowflakeSession])
    val conn = Mockito.mock(classOf[Connection])
    Mockito.when(conn.isClosed).thenReturn(false)
    Mockito.when(session.getConnection).thenReturn(conn)
    val rs = Mockito.mock(classOf[ResultSet])
    val meta = Mockito.mock(classOf[ResultSetMetaData])
    Mockito.when(rs.getMetaData).thenReturn(meta)
    Mockito.when(meta.getColumnCount).thenReturn(1)
    Mockito.when(meta.getColumnLabel(1)).thenReturn("C0")
    Mockito.when(meta.getColumnTypeName(1)).thenReturn("INTEGER")
    Mockito.when(meta.getColumnType(1)).thenReturn(java.sql.Types.INTEGER)
    Mockito.when(meta.isSigned(1)).thenReturn(true)
    Mockito
      .when(session.getColumnFields(any(), anyInt()))
      .thenReturn(java.util.Collections.emptyList[FieldMetadata]())
    Mockito.when(rs.next()).thenReturn(true, false)
    Mockito.when(rs.getObject(1)).thenReturn(Integer.valueOf(5), Nil: _*)
    Mockito.when(rs.getInt(1)).thenReturn(5)
    val statement = Mockito.mock(classOf[Statement])
    Mockito.when(statement.getResultSet).thenReturn(rs)
    val (iterator, _) = new ServerConnection(Map.empty, true, Some(session))
      .resultSetToIterator(statement)
    assert(iterator.next() == Row(5))
  }
}
