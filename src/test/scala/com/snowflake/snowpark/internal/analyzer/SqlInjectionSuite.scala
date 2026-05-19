package com.snowflake.snowpark.internal.analyzer

import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for SNOW-3511808: SQL injection via unescaped field name in
 * `subfieldExpression`, and an over-permissive pre-quoted pass-through in `singleQuote`.
 *
 * Both helpers share a single guiding principle: respect the long-standing API contract that
 * callers may pre-escape their input, but never let a non-well-formed input through unescaped (the
 * injection vector). The shared gate is [[hasOnlyEscapedQuotes]] (for `subfieldExpression` field
 * bodies) and [[isProperlyQuoted]] (for `singleQuote` whole-literal inputs).
 *
 * `subfieldExpression` historically embedded the caller-supplied field name directly between single
 * quotes inside `[...]`. The documented contract is that callers escape embedded single quotes
 * themselves (see the comment in `ColumnSuite.subfield`: "User need to escape single quote with two
 * single quotes"). A field name that violates that contract — i.e. contains unescaped `'` — could
 * close the literal early and inject arbitrary SQL. The fix gates on [[hasOnlyEscapedQuotes]]:
 * well-formed escaped bodies pass through verbatim, malformed bodies have their `'` doubled before
 * wrapping.
 *
 * `singleQuote` historically returned any input that started AND ended with `'` unchanged, on the
 * assumption that the caller had already produced a safe literal. That trusted strings like `'a';
 * DROP TABLE x; --'` — well-bounded but with an unescaped interior `'` that breaks out of the
 * literal scope when emitted into SQL. The fix tightens the early-return so it only fires on
 * properly-quoted inputs (interior `'` characters all doubled), and the wrapping branch now
 * consistently escapes embedded quotes. Inputs that genuinely arrive pre-quoted (e.g. file-format
 * option values like `FIELD_DELIMITER -> "'aa'"`, `PATTERN -> "'.*\\.csv'"`) continue to pass
 * through, which preserves the long-standing API contract.
 */
class SqlInjectionSuite extends AnyFunSuite {

  // ---- subfieldExpression --------------------------------------------------

  test("subfieldExpression escapes embedded single quotes in field name") {
    // Without escaping, this payload (lifted from the SNOW-3511808 ticket)
    // produced: data['x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y']
    // — a well-formed, injected SQL fragment that runs the embedded SELECT
    // outside of any string literal.
    val payload = "x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y"
    val sql = subfieldExpression("data", payload)
    // After the fix, the entire payload is contained in a single SQL string
    // literal: each of the two embedded `'` characters in the payload is
    // doubled to `''`, so the literal-delimiting `'` characters are exactly
    // the outer two added by subfieldExpression.
    val expected =
      "data['x''] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || [''y']"
    assert(sql == expected)
    // Each `'` from the payload is now doubled, giving 2*2 = 4 internal
    // single quotes plus 2 outer wrapping quotes = 6 total.
    assert(sql.count(_ == '\'') == 6)
  }

  test("subfieldExpression preserves field names that contain no quotes") {
    assert(subfieldExpression("data", "name") == "data['name']")
    assert(subfieldExpression("data", "first name") == "data['first name']")
    assert(subfieldExpression("data", "field.with.dots") == "data['field.with.dots']")
  }

  test("subfieldExpression doubles a single embedded quote") {
    // Real-world legitimate case: a field name like O'Brien. Single stray `'`
    // is not well-formed, so the helper doubles it on the caller's behalf.
    assert(subfieldExpression("data", "O'Brien") == "data['O''Brien']")
  }

  test("subfieldExpression doubles multiple unescaped embedded quotes") {
    // Stray `'` characters that are NOT doubled are escaped by the helper.
    assert(subfieldExpression("data", "a'b'c") == "data['a''b''c']")
    assert(subfieldExpression("data", "x'y") == "data['x''y']")
  }

  test("subfieldExpression passes well-formed escaped bodies through unchanged") {
    // Documented API contract (ColumnSuite.scala:497): callers may escape
    // their own single quotes. If the input is already well-formed (every `'`
    // doubled), it is embedded verbatim — re-escaping would double-escape
    // and corrupt the field name.
    assert(subfieldExpression("data", "date with '' and .") == "data['date with '' and .']")
    assert(subfieldExpression("data", "O''Brien") == "data['O''Brien']")
    // The two-character input `''` is itself a well-formed escaped body
    // representing the single-character field name `'`.
    assert(subfieldExpression("data", "''") == "data['''']")
    // To address a field name literally `''`, the caller pre-escapes both
    // quotes, producing the four-character input `''''`.
    assert(subfieldExpression("data", "''''") == "data['''''']")
  }

  // ---- singleQuote ---------------------------------------------------------

  test("singleQuote wraps and escapes plain unquoted values") {
    assert(singleQuote("abc") == "'abc'")
    assert(singleQuote("") == "''")
    assert(singleQuote("hello world") == "'hello world'")
  }

  test("singleQuote escapes embedded quotes when wrapping unquoted input") {
    assert(singleQuote("O'Brien") == "'O''Brien'")
    assert(singleQuote("a'b'c") == "'a''b''c'")
  }

  test("singleQuote passes through properly pre-quoted values unchanged") {
    // This is the API contract that file-format option callers depend on:
    // a caller may supply either a bare value or a pre-formatted SQL literal.
    assert(singleQuote("'aa'") == "'aa'")
    assert(singleQuote("'hello world'") == "'hello world'")
    // A pre-quoted value with its internal `'` properly doubled is also a
    // well-formed literal and passes through verbatim.
    assert(singleQuote("'O''Brien'") == "'O''Brien'")
  }

  test("singleQuote re-wraps inputs whose interior quotes are unbalanced") {
    // Bounded by `'` but the interior `'` is not doubled, so the value is
    // NOT a well-formed literal. Pre-fix, this passed through verbatim and
    // would break out of the literal scope when emitted into SQL. Post-fix,
    // it is treated as an unquoted value and re-wrapped.
    val payload = "'a'; DROP TABLE x; --'"
    val sql = singleQuote(payload)
    val expected = "'''a''; DROP TABLE x; --'''"
    assert(sql == expected)
    // Sanity: the result is one well-formed literal whose content equals the
    // original payload. Snowflake's lexer reads only the outer two `'`s as
    // string-literal delimiters; every other `'` in the result is part of a
    // doubled-quote escape pair `''`.
  }

  test("singleQuote defends against a closing-quote injection payload") {
    // Adversarial payload: try to terminate the literal early and append SQL.
    // Not bounded by `'` so the early-return cannot fire; falls to the
    // wrap-and-escape branch.
    val payload = "'); DROP TABLE users; --"
    val sql = singleQuote(payload)
    val expected = "'''); DROP TABLE users; --'"
    assert(sql == expected)
  }

  // ---- isProperlyQuoted (gate on singleQuote's pass-through branch) --------

  test("isProperlyQuoted accepts well-formed literals") {
    assert(isProperlyQuoted("''")) // empty literal
    assert(isProperlyQuoted("'a'"))
    assert(isProperlyQuoted("'hello world'"))
    assert(isProperlyQuoted("'O''Brien'"))
    assert(isProperlyQuoted("'a''b''c'"))
    assert(isProperlyQuoted("''''")) // literal containing a single `'`
  }

  test("isProperlyQuoted rejects malformed literals") {
    assert(!isProperlyQuoted("")) // too short
    assert(!isProperlyQuoted("'")) // too short
    assert(!isProperlyQuoted("abc")) // no boundaries
    assert(!isProperlyQuoted("'abc")) // unbalanced
    assert(!isProperlyQuoted("abc'")) // unbalanced
    assert(!isProperlyQuoted("'a'b'c'")) // interior `'` not doubled
    assert(!isProperlyQuoted("'a'; DROP TABLE x; --'")) // injection-shaped
  }

  // ---- escapeSingleQuotes (root primitive) ---------------------------------

  test("escapeSingleQuotes doubles every single quote and leaves other chars alone") {
    assert(escapeSingleQuotes("") == "")
    assert(escapeSingleQuotes("abc") == "abc")
    assert(escapeSingleQuotes("'") == "''")
    assert(escapeSingleQuotes("''") == "''''")
    assert(escapeSingleQuotes("a'b") == "a''b")
    assert(escapeSingleQuotes("don't") == "don''t")
  }

  // ---- boundary tests requested in PR review (SNOW-3511808) ----------------

  test("singleQuote: empty literal '' passes through unchanged") {
    // Boundary case: the two-character input `''` is a well-formed empty SQL
    // string literal. `isProperlyQuoted` accepts it, so it round-trips as-is.
    // Without this guarantee, a caller that explicitly passed the empty
    // literal would see it re-wrapped to `''''` (a literal containing one
    // single quote), changing its semantics.
    assert(singleQuote("''") == "''")
  }

  test("subfieldExpression: Int overload needs no escaping") {
    // The `(expr: String, field: Int)` overload addresses array/struct fields
    // by ordinal. Integers cannot carry quote characters and cannot escape
    // the surrounding `[...]` syntax, so this overload deliberately bypasses
    // the string-quoting path and emits the integer directly. This test is
    // here so a future reader of `SqlInjectionSuite` does not wonder whether
    // the overload was accidentally left unprotected.
    assert(subfieldExpression("data", 0) == "data[0]")
    assert(subfieldExpression("data", 42) == "data[42]")
    assert(subfieldExpression("data", -1) == "data[-1]")
  }

  // ---- call-site coverage: confirm callers of singleQuote are protected ----
  //
  // The fix to `singleQuote` defends every call site that funnels user input
  // through it. We assert this end-to-end for each known call site in
  // `package.scala`: `collateExpression`, `selectFromPathWithFormatStatement`,
  // and `copyIntoTable`. Each test uses an adversarial input that would
  // previously have terminated the literal early and injected SQL; the fix
  // contains the entire payload inside a single well-formed string literal.

  test("collateExpression escapes an injected collation spec (SNOW-3511808 side-fix)") {
    // collationSpec flows from `Column.collate(String)` — a public API. Pre-fix
    // `singleQuote` would have passed `"en_US'; DROP TABLE x; --"` through with
    // only the bare wrap, embedding raw `'` characters into the emitted SQL
    // and breaking out of the literal scope on the second `'`.
    val sql = collateExpression("col", "en_US'; DROP TABLE x; --")
    assert(sql == "col COLLATE 'en_US''; DROP TABLE x; --'")
  }

  test("selectFromPathWithFormatStatement escapes injected formatName and pattern") {
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = "@stage",
      formatName = Some("MY_FMT'; DROP TABLE x; --"),
      pattern = Some(".*csv'; DROP TABLE y; --"))
    // Asserting the exact emitted string is brittle (the surrounding builder
    // is dense with spaces); the security property is that both option values
    // were funneled through `singleQuote`, doubled their interior `'`, and
    // landed in the SQL as well-formed single-quoted literals.
    assert(sql.contains("'MY_FMT''; DROP TABLE x; --'"))
    assert(sql.contains("'.*csv''; DROP TABLE y; --'"))
    // And nothing got past with a raw, unescaped quote in the payload region.
    assert(!sql.contains("'MY_FMT';"))
    assert(!sql.contains("'.*csv';"))
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
    // The injected `'` inside the pattern is doubled, so the payload stays
    // contained inside a single well-formed SQL string literal.
    assert(sql.contains("'.*csv''; DROP TABLE x; --'"))
    // Same property as above: the unescaped `'.*csv';` substring (which
    // would indicate the literal was terminated early at the injection
    // point) must not appear anywhere in the emitted SQL.
    assert(!sql.contains("'.*csv';"))
  }
}
