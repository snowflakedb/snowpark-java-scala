package com.snowflake.snowpark.internal

import net.snowflake.client.jdbc.{SnowflakeConnectionV1, SnowflakeResultSetMetaData}
import org.mockito.Mockito.{doThrow, mock, withSettings, when}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{ResultSet, ResultSetMetaData, Statement}

class ServerConnectionSuite extends AnyFunSuite {

  test("resultSetToIterator suppresses close failure under read failure") {
    val connection = mock(classOf[SnowflakeConnectionV1])
    when(connection.isClosed).thenReturn(false)

    val metadata = mock(
      classOf[ResultSetMetaData],
      withSettings().extraInterfaces(classOf[SnowflakeResultSetMetaData]))
    when(metadata.getColumnCount).thenReturn(0)

    val readFailure = new AssertionError("read failed")
    val closeFailure = new AssertionError("close failed")
    val resultSet = mock(classOf[ResultSet])
    when(resultSet.getMetaData).thenReturn(metadata)
    when(resultSet.next()).thenThrow(readFailure)

    val statement = mock(classOf[Statement])
    when(statement.getResultSet).thenReturn(resultSet)
    doThrow(closeFailure).when(statement).close()

    val serverConnection = new ServerConnection(Map.empty, isScalaAPI = true, Some(connection))
    val thrown = intercept[AssertionError] {
      serverConnection.resultSetToIterator(statement)
    }

    assert(thrown eq readFailure)
    assert(thrown.getSuppressed.sameElements(Array(closeFailure)))
  }
}
