package com.snowflake.snowpark.internal.analyzer

import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for SNOW-3511808: SQL injection via unescaped field name in
 * `subfieldExpression`, and an over-permissive pre-quoted pass-through in `singleQuote`.
 *
 * Both helpers share a single guiding principle: respect the long-standing API contract that
 * callers may pre-escape their input, but never let a non-well-formed input through unescaped (the
 * injection vector). The gates are [[hasOnlyEscapedQuotes]] (for `subfieldExpression` field bodies)
 * and [[isProperlyQuoted]] (for `singleQuote` whole-literal inputs). Inputs that don't pass the
 * gate go through [[dollarQuoteOrEscape]], which wraps the value in Snowflake's dollar-quoted
 * string syntax (`$$…$$`) — a delimiter that performs NO internal escape interpretation. That
 * eliminates the SNOW-3511808 injection family in one stroke without our having to enumerate
 * Snowflake's lexer escape table.
 *
 * `subfieldExpression` historically embedded the caller-supplied field name directly between single
 * quotes inside `[...]`. The documented contract is that callers escape embedded single quotes
 * themselves (see the comment in `ColumnSuite.subfield`: "User need to escape single quote with two
 * single quotes"). A field name that violates that contract — i.e. contains unescaped `'` — could
 * close the literal early and inject arbitrary SQL. Post-fix, pre-escaped bodies still pass through
 * wrapped in `'…'` for backward compat; everything else is dollar-quoted, so neither `'` nor `\'`
 * (the backslash-quote-escape variant identified in PR #281 review by
 * @sfc-gh-heshah)
 *   can break out of the literal.
 *
 * `singleQuote` historically returned any input that started AND ended with `'` unchanged, on the
 * assumption that the caller had already produced a safe literal. That trusted strings like `'a';
 * DROP TABLE x; --'` — well-bounded but with an unescaped interior `'` that breaks out of the
 * literal scope when emitted into SQL. The fix tightens the early-return so it only fires on
 * properly-quoted inputs (interior `'` characters all doubled), and the wrapping branch now
 * dollar-quotes the value. Inputs that genuinely arrive pre-quoted (e.g. file-format option values
 * like `FIELD_DELIMITER -> "'aa'"`, `PATTERN -> "'.*\\.csv'"`) continue to pass through, which
 * preserves the long-standing API contract.
 *
 * Why dollar-quoting (and not extended single-quote escaping)? Snowflake's single-quoted lexer
 * interprets `\` as an escape character (`\'` is an alternative encoding of `'`, `\b` is BACKSPACE,
 * etc.), so any quotes-only escape strategy is structurally bypassable. Dollar-quoted strings
 * perform NO escape interpretation — only `$$` terminates the literal — so the fix becomes
 * structural rather than character-by-character. The only edge cases are payloads that literally
 * contain `$$` or end with `$`; those flow through the [[escapeForSingleQuotedLiteral]] fallback
 * path (with both `\` and `'` escaped) and remain safe.
 */
class SqlInjectionSuite extends AnyFunSuite {

  // ---- subfieldExpression --------------------------------------------------

  test("subfieldExpression contains the original SNOW-3511808 payload via dollar-quoting") {
    // Without escaping, this payload (lifted from the SNOW-3511808 ticket)
    // produced: data['x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y']
    // — a well-formed, injected SQL fragment that runs the embedded SELECT
    // outside of any string literal.
    val payload = "x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y"
    val sql = subfieldExpression("data", payload)
    // After the fix, the entire payload is contained in a single dollar-quoted
    // SQL string literal — `'` and `\` characters in the payload need no
    // escaping inside `$$…$$`. The literal-delimiting tokens are exactly the
    // outer `$$` pairs added by subfieldExpression.
    assert(sql == "data[$$x'] || (SELECT SYSTEM$CANCEL_ALL_QUERIES()) || ['y$$]")
    // Sanity: the two `'` characters from the payload appear verbatim, and
    // they are NOT literal-delimiters (the delimiters are `$$`).
    assert(sql.count(_ == '\'') == 2)
  }

  test("subfieldExpression preserves plain field names via single-quote pass-through") {
    // Plain field names contain no `'`, so they satisfy `hasOnlyEscapedQuotes`
    // trivially and take the pass-through path: emitted as `['name']`, the
    // SAME SQL as before the fix. This minimises textual churn in downstream
    // assertions and only switches to dollar-quoting for inputs that actually
    // need it (i.e. inputs with unescaped `'` — the injection vector).
    assert(subfieldExpression("data", "name") == "data['name']")
    assert(subfieldExpression("data", "first name") == "data['first name']")
    assert(subfieldExpression("data", "field.with.dots") == "data['field.with.dots']")
  }

  test("subfieldExpression dollar-quotes field names with unescaped single quotes") {
    // Real-world legitimate case: a field name like O'Brien. Pre-fix this had
    // to be quote-escaped to `O''Brien`; post-fix the unescaped `'` trips
    // `hasOnlyEscapedQuotes`, the field flows through `dollarQuoteOrEscape`,
    // and the `'` simply rides inside the `$$…$$` literal verbatim.
    assert(subfieldExpression("data", "O'Brien") == "data[$$O'Brien$$]")
    assert(subfieldExpression("data", "a'b'c") == "data[$$a'b'c$$]")
    assert(subfieldExpression("data", "x'y") == "data[$$x'y$$]")
  }

  test("subfieldExpression neutralises the backslash-quote-escape bypass via dollar-quoting") {
    // PR #281 review (@sfc-gh-heshah) bypass: `\'` is an alternative single-
    // quote encoding inside Snowflake's single-quoted literals. A payload
    // containing `\'` AND unescaped `'` would defeat a quotes-only escape
    // strategy. With the dollar-quoted wrap branch, `\` is just an ordinary
    // character — the bypass is structurally impossible.
    assert(
      subfieldExpression("data", "\\'] || (SELECT 1) || ['") ==
        "data[$$\\'] || (SELECT 1) || ['$$]")
    // Backslash inside a payload that already trips `hasOnlyEscapedQuotes`
    // (via its unescaped `'`) rides inside `$$…$$` unchanged.
    assert(subfieldExpression("data", "a\\nb'c") == "data[$$a\\nb'c$$]")
  }

  test("subfieldExpression backslash-only inputs are passed through unchanged (known wart)") {
    // Known limitation: a backslash-only input (no unescaped `'`) takes the
    // pass-through path because `hasOnlyEscapedQuotes` only inspects `'`
    // characters. The emitted SQL is `'C:\path'`, where Snowflake's single-
    // quoted lexer will interpret `\p` as "backslash followed by unknown
    // escape, dropped" — i.e. data corruption (the literal becomes `C:path`).
    //
    // This is a pre-existing wart, not a SNOW-3511808 regression: pre-fix
    // behaviour was identical. It is NOT a SQL-injection vector (no `'` or
    // `$$` means the literal cannot be broken out of). A caller who needs a
    // literal backslash in a field name should include an unescaped `'` in
    // the same string (forcing dollar-quoting), or switch to one of the
    // public APIs that funnel input through `singleQuote` (which uses the
    // stricter `isProperlyQuoted` gate and dollar-quotes backslash-only
    // inputs correctly).
    assert(subfieldExpression("data", "C:\\path") == "data['C:\\path']")
    assert(subfieldExpression("data", "a\\nb") == "data['a\\nb']")
  }

  test("subfieldExpression passes well-formed escaped bodies through unchanged") {
    // Documented API contract (ColumnSuite.scala): callers may pre-escape
    // their own single quotes. If the input is already well-formed (every `'`
    // doubled), it is embedded verbatim wrapped in `'…'` — preserving the
    // exact SQL previously emitted for such inputs. Re-wrapping with `$$`
    // here would change the textual form (still semantically valid, but
    // unnecessary churn for callers that opted into the contract).
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

  test("singleQuote dollar-quotes plain unquoted values") {
    assert(singleQuote("abc") == "$$abc$$")
    assert(singleQuote("") == "$$$$")
    assert(singleQuote("hello world") == "$$hello world$$")
  }

  test("singleQuote keeps embedded quotes and backslashes literal") {
    // No internal escaping needed inside `$$…$$`.
    assert(singleQuote("O'Brien") == "$$O'Brien$$")
    assert(singleQuote("a'b'c") == "$$a'b'c$$")
    assert(singleQuote("C:\\path\\to\\file") == "$$C:\\path\\to\\file$$")
    assert(singleQuote("line1\\nline2") == "$$line1\\nline2$$")
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
    // NOT a well-formed pre-quoted literal. Pre-fix, this passed through
    // verbatim and would break out of the literal scope when emitted into
    // SQL. Post-fix, it falls to dollar-quoting and the payload is contained.
    val payload = "'a'; DROP TABLE x; --'"
    val sql = singleQuote(payload)
    assert(sql == "$$'a'; DROP TABLE x; --'$$")
  }

  test("singleQuote defends against the original closing-quote injection payload") {
    // Adversarial payload: try to terminate the literal early and append SQL.
    // Not bounded by `'` on both sides so the pass-through cannot fire; falls
    // to dollar-quoting.
    val payload = "'); DROP TABLE users; --"
    val sql = singleQuote(payload)
    assert(sql == "$$'); DROP TABLE users; --$$")
  }

  test("singleQuote defends against the backslash-quote-escape bypass") {
    // PR #281 review (@sfc-gh-heshah). Snowflake's single-quoted lexer reads
    // `\'` as an alternative encoding of `'`, which would have defeated a
    // quotes-only escape strategy. Dollar-quoting sidesteps the issue
    // entirely — `\` is an ordinary character inside `$$…$$`.
    val payload = "\\'; DROP TABLE x; --"
    val sql = singleQuote(payload)
    assert(sql == "$$\\'; DROP TABLE x; --$$")
    // Sanity: the `\` and the `'` from the payload appear verbatim, and the
    // literal delimiters are `$$` not `'`.
    assert(sql.count(_ == '\\') == 1)
    assert(sql.count(_ == '\'') == 1)
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

  // ---- dollarQuoteOrEscape (primary helper for the wrap branch) ------------

  test("dollarQuoteOrEscape uses dollar-quoting for the common case") {
    // No `$$` in the value and no trailing `$` — the default, fast path.
    assert(dollarQuoteOrEscape("") == "$$$$")
    assert(dollarQuoteOrEscape("hello") == "$$hello$$")
    // `'` and `\` ride inside `$$…$$` literally; that's the structural defence.
    assert(dollarQuoteOrEscape("O'Brien") == "$$O'Brien$$")
    assert(dollarQuoteOrEscape("C:\\path") == "$$C:\\path$$")
    assert(dollarQuoteOrEscape("\\'; DROP TABLE x; --") == "$$\\'; DROP TABLE x; --$$")
    // A single mid-string `$` is safe — only `$$` terminates the literal.
    assert(dollarQuoteOrEscape("a$b") == "$$a$b$$")
    // `SYSTEM$VERSION`-style identifiers with single `$` are common; ensure
    // they survive untouched.
    assert(dollarQuoteOrEscape("SYSTEM$VERSION") == "$$SYSTEM$VERSION$$")
  }

  test("dollarQuoteOrEscape falls back to single-quote when value contains $$") {
    // Embedded `$$` would close the dollar-quoted literal early. Fall back to
    // a single-quoted literal (with `'` and `\` escaped) so the payload
    // remains contained.
    assert(dollarQuoteOrEscape("a$$b") == "'a$$b'")
    assert(dollarQuoteOrEscape("$$") == "'$$'")
    assert(dollarQuoteOrEscape("foo$$bar$$baz") == "'foo$$bar$$baz'")
  }

  test("dollarQuoteOrEscape falls back to single-quote when value ends with $") {
    // A trailing `$` adjacent to our closing `$$` would form `$$$`, which
    // the lexer would parse as a closing `$$` followed by an extraneous `$`
    // — a syntax error rather than an injection, but still wrong.
    assert(dollarQuoteOrEscape("$") == "'$'")
    assert(dollarQuoteOrEscape("abc$") == "'abc$'")
    assert(dollarQuoteOrEscape("a$b$") == "'a$b$'")
  }

  test("dollarQuoteOrEscape fallback path still escapes ' and \\ for injection-safety") {
    // When the value forces the single-quote fallback AND contains characters
    // that would attack a single-quoted literal, `escapeForSingleQuotedLiteral`
    // doubles both `\` and `'` so the payload is fully contained.
    // Input: `$$\'; DROP TABLE x; --` — `$$` triggers fallback, `\'` is the
    // backslash-quote-escape bypass.
    assert(
      dollarQuoteOrEscape("$$\\'; DROP TABLE x; --") ==
        "'$$\\\\''; DROP TABLE x; --'")
    // Input: `a$$b'c` — `$$` triggers fallback, `'` needs escaping.
    assert(dollarQuoteOrEscape("a$$b'c") == "'a$$b''c'")
  }

  // ---- escapeForSingleQuotedLiteral (fallback-path primitive) --------------

  test("escapeForSingleQuotedLiteral doubles every single quote and leaves plain text alone") {
    assert(escapeForSingleQuotedLiteral("") == "")
    assert(escapeForSingleQuotedLiteral("abc") == "abc")
    assert(escapeForSingleQuotedLiteral("'") == "''")
    assert(escapeForSingleQuotedLiteral("''") == "''''")
    assert(escapeForSingleQuotedLiteral("a'b") == "a''b")
    assert(escapeForSingleQuotedLiteral("don't") == "don''t")
  }

  test("escapeForSingleQuotedLiteral doubles every backslash") {
    // Snowflake's single-quoted lexer interprets `\` as an escape character
    // (e.g. `\b` → BACKSPACE, `\'` → `'`). Doubling `\` to `\\` makes the
    // lexer re-read it as a literal `\`, which is the only way to faithfully
    // round-trip a value that contains backslashes through a single-quoted
    // literal (the fallback path).
    assert(escapeForSingleQuotedLiteral("\\") == "\\\\")
    assert(escapeForSingleQuotedLiteral("a\\b") == "a\\\\b")
    assert(escapeForSingleQuotedLiteral("\\\\") == "\\\\\\\\")
    // `\n` (two characters: backslash + n) becomes `\\n` so Snowflake
    // re-reads it as `\` followed by `n`, not the single newline character.
    assert(escapeForSingleQuotedLiteral("\\n") == "\\\\n")
  }

  test("escapeForSingleQuotedLiteral escapes backslash THEN quote (mixed input)") {
    // Mixed payload covering the SNOW-3511808 backslash bypass: a `'`
    // preceded by a `\`. Quotes-only escaping would have produced `\''`,
    // which Snowflake reads as `\'` (alternative quote-escape) followed by
    // a stray `'` that closes the literal early. Doubling `\` first yields
    // `\\''`, which Snowflake reads as `\` followed by a doubled-quote
    // escape — the literal stays closed by exactly its outer `'`s.
    assert(escapeForSingleQuotedLiteral("\\'") == "\\\\''")
    assert(escapeForSingleQuotedLiteral("'\\") == "''\\\\")
    assert(escapeForSingleQuotedLiteral("a\\'b") == "a\\\\''b")
  }

  // ---- boundary tests requested in earlier PR review -----------------------

  test("singleQuote: empty literal '' passes through unchanged") {
    // Boundary case: the two-character input `''` is a well-formed empty SQL
    // string literal. `isProperlyQuoted` accepts it, so it round-trips as-is
    // (instead of being re-wrapped to `$$$$`, which would be semantically
    // identical but textually different).
    assert(singleQuote("''") == "''")
  }

  test("subfieldExpression: Int overload needs no escaping") {
    // The `(expr: String, field: Int)` overload addresses array/struct fields
    // by ordinal. Integers cannot carry quote characters and cannot escape
    // the surrounding `[...]` syntax, so this overload deliberately bypasses
    // the literal-wrapping path and emits the integer directly. This test is
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
  // contains the entire payload inside a single dollar-quoted string literal.

  test("collateExpression dollar-quotes an injected collation spec") {
    // collationSpec flows from `Column.collate(String)` — a public API. Pre-fix
    // `singleQuote` would have passed `"en_US'; DROP TABLE x; --"` through with
    // only the bare wrap, embedding raw `'` characters into the emitted SQL
    // and breaking out of the literal scope on the second `'`.
    val sql = collateExpression("col", "en_US'; DROP TABLE x; --")
    assert(sql == "col COLLATE $$en_US'; DROP TABLE x; --$$")
  }

  test("selectFromPathWithFormatStatement dollar-quotes injected formatName and pattern") {
    val sql = selectFromPathWithFormatStatement(
      project = Seq.empty,
      path = "@stage",
      formatName = Some("MY_FMT'; DROP TABLE x; --"),
      pattern = Some(".*csv'; DROP TABLE y; --"))
    // Asserting the exact emitted string is brittle (the surrounding builder
    // is dense with spaces); the security property is that both option values
    // were funneled through `singleQuote`, dollar-quoted, and landed in the
    // SQL as well-formed `$$…$$` literals.
    assert(sql.contains("$$MY_FMT'; DROP TABLE x; --$$"))
    assert(sql.contains("$$.*csv'; DROP TABLE y; --$$"))
    // And nothing got past with a raw, unescaped quote in the payload region
    // (the substring that would indicate early literal termination).
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
    // The injected `'` rides inside the `$$…$$` literal verbatim — no
    // escaping needed, no way to break out.
    assert(sql.contains("$$.*csv'; DROP TABLE x; --$$"))
    // Same property as above: the early-termination substring must not appear.
    assert(!sql.contains("'.*csv';"))
  }
}
