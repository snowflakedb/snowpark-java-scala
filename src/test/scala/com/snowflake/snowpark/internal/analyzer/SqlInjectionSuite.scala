package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for the SQL injection class introduced by unescaped `'` in `subfieldExpression`
 * and an over-permissive pre-quoted pass-through in `singleQuote`. The fix escapes backslashes and
 * doubles embedded single quotes before wrapping, and the well-formedness check models Snowflake's
 * backslash escaping (so the `\\` desync can no longer slip a closing quote through). Pre-quoted
 * well-formed literals still pass through verbatim so the long-standing file-format-option contract
 * (and downstream string-equality checks in `DataFrameWriter`) keeps working.
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

  test("subfieldExpression neutralises the backslash closing-quote desync payload") {
    // Pre-fix, `hasOnlyEscapedSingleQuotes` mis-parsed `\\` and passed this through verbatim,
    // letting Snowflake read `\\` as a backslash and `'` as a real closing quote -> the embedded
    // scalar subquery executed. The field must now take the escape branch (not verbatim).
    val field = "\\\\'||(SELECT c FROM secret)||''"
    val sql = subfieldExpression("data", field)
    assert(sql == "data['" + escapeSingleQuotesForSingleQuotedLiteral(field) + "']")
    assert(sql != "data['" + field + "']") // proves the verbatim pass-through was rejected
  }

  test("subfieldExpression escapes lone and trailing backslashes") {
    // A trailing/dangling backslash would otherwise escape the wrapper's closing quote, so these
    // are rejected by the well-formedness check and routed through the escape branch.
    assert(subfieldExpression("data", "x\\") == "data['x\\\\']")
    assert(subfieldExpression("data", "\\") == "data['\\\\']")
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

  test("singleQuote escapes backslashes so they cannot escape the wrapper quote") {
    // A lone backslash would otherwise turn the closing wrapper `'` into an escaped quote.
    assert(singleQuote("\\") == "'\\\\'")
    // Non-well-formed inputs go through the escape branch; assert they are fully escaped.
    val payload = "\\' || (SELECT c FROM no_such) || '"
    assert(singleQuote(payload) == "'" + escapeSingleQuotesForSingleQuotedLiteral(payload) + "'")
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

  test("isStringLiteralProperlySingleQuoted models Snowflake backslash escaping") {
    // `'\'` ends in a dangling backslash that escapes the closing quote -> not well-formed.
    // Pre-fix this was accepted because `\` was only treated as an escape when followed by `'`.
    assert(!isStringLiteralProperlySingleQuoted("'\\'"))
    // `'\\'` is a genuine, closed backslash literal -> well-formed.
    assert(isStringLiteralProperlySingleQuoted("'\\\\'"))
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

  test("escapeSingleQuotesForSingleQuotedLiteral doubles backslashes before quotes") {
    assert(escapeSingleQuotesForSingleQuotedLiteral("\\") == "\\\\")
    assert(escapeSingleQuotesForSingleQuotedLiteral("a\\b") == "a\\\\b")
    // backslash then quote: backslash doubled first, then the quote doubled.
    assert(escapeSingleQuotesForSingleQuotedLiteral("\\'") == "\\\\''")
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

  // ---- substring_index SQL injection (regression) ---------------

  test("substring_index does not emit UnresolvedAttribute with raw SQL for injection payload") {
    import com.snowflake.snowpark.functions.substring_index
    val payload = "x')||(SELECT LISTAGG(secret,',') FROM secrets)||('y"
    val result = substring_index(payload, ",", -1)
    def containsUnsafeUnresolved(e: Expression): Boolean = e match {
      case UnresolvedAttribute(name) => name.contains("reverse(")
      case _ => e.children.exists(containsUnsafeUnresolved)
    }
    assert(
      !containsUnsafeUnresolved(result.expr),
      "substring_index must not interpolate str into raw SQL via UnresolvedAttribute")
  }

  test("substring_index wraps str in Literal for both positive and negative count") {
    import com.snowflake.snowpark.functions.substring_index
    val str = "a'b"
    val negResult = substring_index(str, ",", -1)
    val posResult = substring_index(str, ",", 1)

    def containsLiteralWithValue(e: Expression, value: String): Boolean = e match {
      case Literal(v, _) => v == value
      case _ => e.children.exists(containsLiteralWithValue(_, value))
    }

    assert(
      containsLiteralWithValue(negResult.expr, str),
      "negative count path must wrap str in Literal")
    assert(
      containsLiteralWithValue(posResult.expr, str),
      "positive count path must wrap str in Literal")
  }
  // ---- DataTypeMapper.toSql IntegerType injection (regression) ---------------

  test("toSql rejects non-Int value in IntegerType column (injection payload)") {
    val payload = "0 :: int) AS T(\"A\")) UNION ALL SELECT CURRENT_ROLE()) --"
    intercept[UnsupportedOperationException] {
      DataTypeMapper.toSql(payload, Some(IntegerType))
    }
  }

  test("toSql renders legitimate Int values for IntegerType unchanged") {
    assert(DataTypeMapper.toSql(42, Some(IntegerType)) == "42 :: int")
    assert(DataTypeMapper.toSql(0, Some(IntegerType)) == "0 :: int")
    assert(DataTypeMapper.toSql(-1, Some(IntegerType)) == "-1 :: int")
    assert(DataTypeMapper.toSql(Int.MaxValue, Some(IntegerType)) == s"${Int.MaxValue} :: int")
  }
}
