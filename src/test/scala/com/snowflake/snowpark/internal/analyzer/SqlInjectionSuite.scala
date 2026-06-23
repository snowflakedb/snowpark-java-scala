package com.snowflake.snowpark.internal.analyzer

import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for the SQL injection class introduced by unescaped `'` in `subfieldExpression`
 * and an over-permissive pre-quoted pass-through in `singleQuote`. The fix doubles embedded single
 * quotes before wrapping; pre-quoted well-formed literals pass through verbatim so the
 * long-standing file-format-option contract (and downstream string-equality checks in
 * `DataFrameWriter`) keeps working.
 */
class SqlInjectionSuite extends AnyFunSuite {

  // ---- subfieldExpression --------------------------------------------------

  test("subfieldExpression neutralises the original closing-quote injection payload") {
    val payload = "x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y"
    val sql = subfieldExpression("data", payload)
    assert(sql == "data['x''] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || [''y']")
  }

  test("subfieldExpression preserves plain field names") {
    assert(subfieldExpression("data", "name") == "data['name']")
    assert(subfieldExpression("data", "first name") == "data['first name']")
    assert(subfieldExpression("data", "field.with.dots") == "data['field.with.dots']")
  }

  test("subfieldExpression doubles embedded single quotes") {
    assert(subfieldExpression("data", "O'Brien") == "data['O''Brien']")
    assert(subfieldExpression("data", "a'b'c") == "data['a''b''c']")
    assert(subfieldExpression("data", "x'y") == "data['x''y']")
  }

  test("subfieldExpression passes well-formed escaped bodies through unchanged") {
    // Documented API contract (ColumnSuite.subfield): callers may pre-escape `'` themselves.
    assert(subfieldExpression("data", "date with '' and .") == "data['date with '' and .']")
    assert(subfieldExpression("data", "O''Brien") == "data['O''Brien']")
    // `''` is the well-formed escaped body for a one-character field name `'`; `''''` for `''`.
    assert(subfieldExpression("data", "''") == "data['''']")
    assert(subfieldExpression("data", "''''") == "data['''''']")
  }

  test("subfieldExpression: Int overload needs no escaping") {
    assert(subfieldExpression("data", 0) == "data[0]")
    assert(subfieldExpression("data", 42) == "data[42]")
    assert(subfieldExpression("data", -1) == "data[-1]")
  }

  // ---- singleQuote ---------------------------------------------------------

  test("singleQuote wraps plain values in single quotes") {
    assert(singleQuote("abc") == "'abc'")
    assert(singleQuote("") == "''")
    assert(singleQuote("hello world") == "'hello world'")
    assert(singleQuote("index") == "'index'")
    assert(singleQuote("name") == "'name'")
    assert(singleQuote("SYSTEM$VERSION") == "'SYSTEM$VERSION'")
  }

  test("singleQuote doubles embedded single quotes") {
    assert(singleQuote("O'Brien") == "'O''Brien'")
    assert(singleQuote("a'b'c") == "'a''b''c'")
  }

  test("singleQuote passes through properly pre-quoted values unchanged") {
    // API contract for file-format option callers: bare value or pre-formatted literal.
    assert(singleQuote("'aa'") == "'aa'")
    assert(singleQuote("'hello world'") == "'hello world'")
    assert(singleQuote("'O''Brien'") == "'O''Brien'")
    assert(singleQuote("'O\\'Brien'") == "'O\\'Brien'")
  }

  test("singleQuote: empty literal '' passes through unchanged") {
    assert(singleQuote("''") == "''")
  }

  test("singleQuote re-wraps inputs whose interior quotes are unbalanced") {
    // Pre-fix this passed through verbatim and broke out of the literal scope.
    assert(singleQuote("'a'; DROP TABLE x; --'") == "'''a''; DROP TABLE x; --'''")
  }

  test("singleQuote defends against the original closing-quote injection payload") {
    assert(singleQuote("'); DROP TABLE users; --") == "'''); DROP TABLE users; --'")
  }

  // ---- isStringLiteralProperlySingleQuoted (singleQuote pass-through gate) ----

  test("isStringLiteralProperlySingleQuoted accepts well-formed literals") {
    assert(isStringLiteralProperlySingleQuoted("''"))
    assert(isStringLiteralProperlySingleQuoted("'a'"))
    assert(isStringLiteralProperlySingleQuoted("'hello world'"))
    assert(isStringLiteralProperlySingleQuoted("'O''Brien'"))
    assert(isStringLiteralProperlySingleQuoted("'a''b''c'"))
    assert(isStringLiteralProperlySingleQuoted("''''"))
    assert(isStringLiteralProperlySingleQuoted("'O\\'Brien'"))
    assert(isStringLiteralProperlySingleQuoted("'a\\'b\\'c'"))
  }

  test("isStringLiteralProperlySingleQuoted rejects malformed literals") {
    assert(!isStringLiteralProperlySingleQuoted(""))
    assert(!isStringLiteralProperlySingleQuoted("'"))
    assert(!isStringLiteralProperlySingleQuoted("abc"))
    assert(!isStringLiteralProperlySingleQuoted("'abc"))
    assert(!isStringLiteralProperlySingleQuoted("abc'"))
    assert(!isStringLiteralProperlySingleQuoted("'a'b'c'"))
    assert(!isStringLiteralProperlySingleQuoted("'a'; DROP TABLE x; --'"))
  }

  // ---- escapeSingleQuotesForSingleQuotedLiteral (primitive) ---------------

  test("escapeSingleQuotesForSingleQuotedLiteral doubles every single quote") {
    assert(escapeSingleQuotesForSingleQuotedLiteral("") == "")
    assert(escapeSingleQuotesForSingleQuotedLiteral("abc") == "abc")
    assert(escapeSingleQuotesForSingleQuotedLiteral("'") == "''")
    assert(escapeSingleQuotesForSingleQuotedLiteral("''") == "''''")
    assert(escapeSingleQuotesForSingleQuotedLiteral("a'b") == "a''b")
    assert(escapeSingleQuotesForSingleQuotedLiteral("don't") == "don''t")
  }

  // ---- call-site coverage --------------------------------------------------

  test("collateExpression escapes an injected collation spec") {
    val sql = collateExpression("col", "en_US'; DROP TABLE x; --")
    assert(sql == "col COLLATE 'en_US''; DROP TABLE x; --'")
  }

  test("selectFromPathWithFormatStatement escapes injected formatName and pattern") {
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = "@stage",
      formatName = Some("MY_FMT'; DROP TABLE x; --"),
      pattern = Some(".*csv'; DROP TABLE y; --"))
    assert(sql.contains("'MY_FMT''; DROP TABLE x; --'"))
    assert(sql.contains("'.*csv''; DROP TABLE y; --'"))
  }

  test("copyIntoTable escapes an injected COPY INTO pattern") {
    val sql = copyIntoTable(
      tableName = "T",
      filePath = "@stage",
      format = "CSV",
      formatTypeOptions = Map.empty,
      copyOptions = Map.empty,
      pattern = Some(".*csv'; DROP TABLE x; --"),
      columnNames = Seq.empty,
      transformations = Seq.empty)
    assert(sql.contains("'.*csv''; DROP TABLE x; --'"))
  }

  // ---- unicode stage quoting (SNOW-3632537) ---------------------------------

  test("fileOperationStatement quotes PUT stage ref containing non-ASCII") {
    import com.snowflake.snowpark.FileOperationCommand._
    val unicodeStage = """@"DQ"."自動化専用_変更禁止".SNOWPARK_TEMP_STAGE_XYZ"""
    val sql = fileOperationStatement(PutCommand, "file:///tmp/file.csv", unicodeStage, Map.empty)
    assert(
      sql.contains(s"'$unicodeStage'"),
      s"PUT should single-quote non-ASCII stage ref, got: $sql")
  }

  test("fileOperationStatement quotes GET stage ref containing non-ASCII") {
    import com.snowflake.snowpark.FileOperationCommand._
    val unicodeStage = """@"DQ"."自動化専用_変更禁止".SNOWPARK_TEMP_STAGE_XYZ/file.csv"""
    val sql = fileOperationStatement(GetCommand, "file:///tmp/", unicodeStage, Map.empty)
    assert(
      sql.contains(s"'$unicodeStage'"),
      s"GET should single-quote non-ASCII stage ref, got: $sql")
  }

  test("fileOperationStatement leaves ASCII stage ref unchanged") {
    import com.snowflake.snowpark.FileOperationCommand._
    val asciiStage = """@"DB"."SCHEMA".STAGE"""
    val sql = fileOperationStatement(
      PutCommand,
      "file:///tmp/file.csv",
      asciiStage,
      Map("OVERWRITE" -> "TRUE"))
    assert(
      !sql.contains(s"'$asciiStage"),
      s"PUT should not single-quote ASCII-only stage ref, got: $sql")
    assert(sql.contains(asciiStage))
  }

  test("selectFromPathWithFormatStatement quotes path containing non-ASCII") {
    val unicodePath = """@"DB"."日本語".STAGE/dir"""
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = unicodePath,
      formatName = None,
      pattern = None)
    assert(
      sql.contains(s"'$unicodePath'"),
      s"SELECT FROM should single-quote non-ASCII path, got: $sql")
  }

  test("selectFromPathWithFormatStatement leaves ASCII path unchanged") {
    val asciiPath = "@stage/dir/file.csv"
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = asciiPath,
      formatName = None,
      pattern = None)
    assert(
      !sql.contains(s"'$asciiPath"),
      s"SELECT FROM should not single-quote ASCII-only path, got: $sql")
    assert(sql.contains(asciiPath))
  }

  test("copyIntoTable quotes filePath containing non-ASCII") {
    val unicodePath = """@"DB"."日本語テスト".STAGE"""
    val sql = copyIntoTable(
      tableName = "T",
      filePath = unicodePath,
      format = "CSV",
      formatTypeOptions = Map.empty,
      copyOptions = Map.empty,
      pattern = None,
      columnNames = Seq.empty,
      transformations = Seq.empty)
    assert(
      sql.contains(s"'$unicodePath'"),
      s"COPY INTO should single-quote non-ASCII filePath, got: $sql")
  }

  test("copyIntoTable quotes filePath in SELECT transform clause with non-ASCII") {
    val unicodePath = """@"DB"."日本語テスト".STAGE"""
    val sql = copyIntoTable(
      tableName = "T",
      filePath = unicodePath,
      format = "CSV",
      formatTypeOptions = Map.empty,
      copyOptions = Map.empty,
      pattern = None,
      columnNames = Seq.empty,
      transformations = Seq("$1", "$2"))
    assert(
      sql.contains(s"'$unicodePath'"),
      s"COPY INTO with transformations should single-quote non-ASCII filePath, got: $sql")
  }

  test("copyIntoTable leaves ASCII filePath unchanged") {
    val asciiPath = "@stage/dir"
    val sql = copyIntoTable(
      tableName = "T",
      filePath = asciiPath,
      format = "CSV",
      formatTypeOptions = Map.empty,
      copyOptions = Map.empty,
      pattern = None,
      columnNames = Seq.empty,
      transformations = Seq.empty)
    assert(
      !sql.contains(s"'$asciiPath"),
      s"COPY INTO should not single-quote ASCII-only filePath, got: $sql")
    assert(sql.contains(asciiPath))
  }

  test("StagedFileWriter.getCopyIntoLocationQuery quotes non-ASCII stageLocation") {
    val writer = new StagedFileWriter(null)
    writer.stageLocation = """@"DB"."日本語".STAGE"""
    writer.formatType = "CSV"
    val sql = writer.getCopyIntoLocationQuery("SELECT * FROM T")
    assert(
      sql.startsWith("COPY INTO '@\"DB\".\"日本語\".STAGE'"),
      s"COPY INTO location should single-quote non-ASCII stageLocation, got: $sql")
  }

  test("StagedFileWriter.getCopyIntoLocationQuery leaves ASCII stageLocation unchanged") {
    val writer = new StagedFileWriter(null)
    writer.stageLocation = """@"DB"."SCHEMA".STAGE"""
    writer.formatType = "CSV"
    val sql = writer.getCopyIntoLocationQuery("SELECT * FROM T")
    assert(
      sql.startsWith("""COPY INTO @"DB"."SCHEMA".STAGE"""),
      s"COPY INTO location should not single-quote ASCII stageLocation, got: $sql")
  }
}
