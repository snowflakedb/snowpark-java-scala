package com.snowflake.snowpark.internal.analyzer

import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for SNOW-3511808: SQL injection via unescaped `'` in `subfieldExpression`, an
 * over-permissive pre-quoted pass-through in `singleQuote`, and the PR #281 `\'`-escape-bypass
 * variant. The fix routes any input that contains `'` or `\` through dollar-quoting (`$$…$$`),
 * which performs no internal escape interpretation. Safe inputs keep the pre-fix `'value'` form so
 * downstream string-equality comparisons against canonical single-quoted literals (notably
 * `DataFrameWriter`'s `columnOrder` validator) continue to work.
 */
class SqlInjectionSuite extends AnyFunSuite {

  // ---- subfieldExpression --------------------------------------------------

  test("subfieldExpression contains the original SNOW-3511808 payload via dollar-quoting") {
    val payload = "x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y"
    val sql = subfieldExpression("data", payload)
    assert(sql == "data[$$x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y$$]")
    assert(sql.count(_ == '\'') == 2)
  }

  test("subfieldExpression preserves plain field names via single-quote pass-through") {
    assert(subfieldExpression("data", "name") == "data['name']")
    assert(subfieldExpression("data", "first name") == "data['first name']")
    assert(subfieldExpression("data", "field.with.dots") == "data['field.with.dots']")
  }

  test("subfieldExpression dollar-quotes field names with unescaped single quotes") {
    assert(subfieldExpression("data", "O'Brien") == "data[$$O'Brien$$]")
    assert(subfieldExpression("data", "a'b'c") == "data[$$a'b'c$$]")
    assert(subfieldExpression("data", "x'y") == "data[$$x'y$$]")
  }

  test("subfieldExpression neutralises the backslash-quote-escape bypass via dollar-quoting") {
    assert(
      subfieldExpression("data", "\\'] || (SELECT 1) || ['") ==
        "data[$$\\'] || (SELECT 1) || ['$$]")
    assert(subfieldExpression("data", "a\\nb'c") == "data[$$a\\nb'c$$]")
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

  test("singleQuote wraps plain unquoted values in bare single quotes (no escaping needed)") {
    // Plain inputs (no `'`, no `\`) keep their pre-fix `'value'` form so downstream
    // string-equality checks (e.g. DataFrameWriter.columnOrder validator) keep working.
    assert(singleQuote("abc") == "'abc'")
    assert(singleQuote("") == "''")
    assert(singleQuote("hello world") == "'hello world'")
    assert(singleQuote("index") == "'index'")
    assert(singleQuote("name") == "'name'")
    assert(singleQuote("SYSTEM$VERSION") == "'SYSTEM$VERSION'")
    assert(singleQuote("a$$b") == "'a$$b'")
  }

  test("singleQuote dollar-quotes values that contain ' or \\") {
    assert(singleQuote("O'Brien") == "$$O'Brien$$")
    assert(singleQuote("a'b'c") == "$$a'b'c$$")
    assert(singleQuote("C:\\path\\to\\file") == "$$C:\\path\\to\\file$$")
    assert(singleQuote("line1\\nline2") == "$$line1\\nline2$$")
  }

  test("singleQuote passes through properly pre-quoted values unchanged") {
    // API contract for file-format option callers: bare value or pre-formatted literal.
    assert(singleQuote("'aa'") == "'aa'")
    assert(singleQuote("'hello world'") == "'hello world'")
    assert(singleQuote("'O''Brien'") == "'O''Brien'")
  }

  test("singleQuote: empty literal '' passes through unchanged") {
    assert(singleQuote("''") == "''")
  }

  test("singleQuote re-wraps inputs whose interior quotes are unbalanced") {
    // Pre-fix this passed through verbatim and broke out of the literal scope.
    assert(singleQuote("'a'; DROP TABLE x; --'") == "$$'a'; DROP TABLE x; --'$$")
  }

  test("singleQuote defends against the original closing-quote injection payload") {
    assert(singleQuote("'); DROP TABLE users; --") == "$$'); DROP TABLE users; --$$")
  }

  test("singleQuote defends against the backslash-quote-escape bypass") {
    // PR #281: `\'` is Snowflake's alternative encoding of `'` inside single-quoted literals.
    val sql = singleQuote("\\'; DROP TABLE x; --")
    assert(sql == "$$\\'; DROP TABLE x; --$$")
    assert(sql.count(_ == '\\') == 1)
    assert(sql.count(_ == '\'') == 1)
  }

  // ---- isProperlyQuoted (gate on singleQuote's pass-through branch) --------

  test("isProperlyQuoted accepts well-formed literals") {
    assert(isProperlyQuoted("''"))
    assert(isProperlyQuoted("'a'"))
    assert(isProperlyQuoted("'hello world'"))
    assert(isProperlyQuoted("'O''Brien'"))
    assert(isProperlyQuoted("'a''b''c'"))
    assert(isProperlyQuoted("''''"))
  }

  test("isProperlyQuoted rejects malformed literals") {
    assert(!isProperlyQuoted(""))
    assert(!isProperlyQuoted("'"))
    assert(!isProperlyQuoted("abc"))
    assert(!isProperlyQuoted("'abc"))
    assert(!isProperlyQuoted("abc'"))
    assert(!isProperlyQuoted("'a'b'c'"))
    assert(!isProperlyQuoted("'a'; DROP TABLE x; --'"))
  }

  // ---- dollarQuoteOrEscape (primary wrap helper) ---------------------------

  test("dollarQuoteOrEscape uses dollar-quoting for the common case") {
    assert(dollarQuoteOrEscape("") == "$$$$")
    assert(dollarQuoteOrEscape("hello") == "$$hello$$")
    assert(dollarQuoteOrEscape("O'Brien") == "$$O'Brien$$")
    assert(dollarQuoteOrEscape("C:\\path") == "$$C:\\path$$")
    assert(dollarQuoteOrEscape("\\'; DROP TABLE x; --") == "$$\\'; DROP TABLE x; --$$")
    assert(dollarQuoteOrEscape("a$b") == "$$a$b$$")
    assert(dollarQuoteOrEscape("SYSTEM$VERSION") == "$$SYSTEM$VERSION$$")
  }

  test("dollarQuoteOrEscape falls back to single-quote when value contains $$") {
    assert(dollarQuoteOrEscape("a$$b") == "'a$$b'")
    assert(dollarQuoteOrEscape("$$") == "'$$'")
    assert(dollarQuoteOrEscape("foo$$bar$$baz") == "'foo$$bar$$baz'")
  }

  test("dollarQuoteOrEscape falls back to single-quote when value ends with $") {
    // A trailing `$` would combine with our closing `$$` into a malformed `$$$`.
    assert(dollarQuoteOrEscape("$") == "'$'")
    assert(dollarQuoteOrEscape("abc$") == "'abc$'")
    assert(dollarQuoteOrEscape("a$b$") == "'a$b$'")
  }

  test("dollarQuoteOrEscape fallback path still escapes ' and \\ for injection-safety") {
    assert(
      dollarQuoteOrEscape("$$\\'; DROP TABLE x; --") ==
        "'$$\\\\''; DROP TABLE x; --'")
    assert(dollarQuoteOrEscape("a$$b'c") == "'a$$b''c'")
  }

  // ---- escapeForSingleQuotedLiteral (fallback-path primitive) --------------

  test("escapeForSingleQuotedLiteral doubles single quotes") {
    assert(escapeForSingleQuotedLiteral("") == "")
    assert(escapeForSingleQuotedLiteral("abc") == "abc")
    assert(escapeForSingleQuotedLiteral("'") == "''")
    assert(escapeForSingleQuotedLiteral("''") == "''''")
    assert(escapeForSingleQuotedLiteral("a'b") == "a''b")
    assert(escapeForSingleQuotedLiteral("don't") == "don''t")
  }

  test("escapeForSingleQuotedLiteral doubles backslashes") {
    assert(escapeForSingleQuotedLiteral("\\") == "\\\\")
    assert(escapeForSingleQuotedLiteral("a\\b") == "a\\\\b")
    assert(escapeForSingleQuotedLiteral("\\\\") == "\\\\\\\\")
    assert(escapeForSingleQuotedLiteral("\\n") == "\\\\n")
  }

  test("escapeForSingleQuotedLiteral escapes backslash before quote (mixed input)") {
    // Order matters: doubling `\` first prevents `\''` from being read as `\'` + stray `'`.
    assert(escapeForSingleQuotedLiteral("\\'") == "\\\\''")
    assert(escapeForSingleQuotedLiteral("'\\") == "''\\\\")
    assert(escapeForSingleQuotedLiteral("a\\'b") == "a\\\\''b")
  }

  // ---- call-site coverage --------------------------------------------------

  test("collateExpression dollar-quotes an injected collation spec") {
    val sql = collateExpression("col", "en_US'; DROP TABLE x; --")
    assert(sql == "col COLLATE $$en_US'; DROP TABLE x; --$$")
  }

  test("selectFromPathWithFormatStatement dollar-quotes injected formatName and pattern") {
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = "@stage",
      formatName = Some("MY_FMT'; DROP TABLE x; --"),
      pattern = Some(".*csv'; DROP TABLE y; --"))
    assert(sql.contains("$$MY_FMT'; DROP TABLE x; --$$"))
    assert(sql.contains("$$.*csv'; DROP TABLE y; --$$"))
    assert(!sql.contains("'MY_FMT';"))
    assert(!sql.contains("'.*csv';"))
  }

  test("copyIntoTable dollar-quotes an injected COPY INTO pattern") {
    val sql = copyIntoTable(
      tableName = "T",
      filePath = "@stage",
      format = "CSV",
      formatTypeOptions = Map.empty,
      copyOptions = Map.empty,
      pattern = Some(".*csv'; DROP TABLE x; --"),
      columnNames = Seq.empty,
      transformations = Seq.empty)
    assert(sql.contains("$$.*csv'; DROP TABLE x; --$$"))
    assert(!sql.contains("'.*csv';"))
  }
}
