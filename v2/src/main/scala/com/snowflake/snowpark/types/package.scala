package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ErrorMessage

package object types {

  private[snowpark] def quoteName(name: String): String = {
    val alreadyQuoted = "^(\".+\")$".r
    val unquotedCaseInsenstive = "^([_A-Za-z]+[_A-Za-z0-9$]*)$".r
    name.trim match {
      case alreadyQuoted(n) => validateQuotedName(n)
      case unquotedCaseInsenstive(n) =>
        // scalastyle:off caselocale
         doubleQuoteName(escapeQuotes(n.toUpperCase))
      // scalastyle:on caselocale
      case n => doubleQuoteName(escapeQuotes(n))
    }
  }

  private def validateQuotedName(name: String): String = {
    // Snowflake uses 2 double-quote to escape double-quote,
    // if there are any double-quote in the object name, it needs to be 2 double-quote.
    if (name.substring(1, name.length - 1).replaceAll("\"\"", "").contains("\"")) {
      throw ErrorMessage.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
    } else {
      name
    }
  }
  private def doubleQuoteName(name: String): String =
    s""""${name}""""
  private def escapeQuotes(unescaped: String): String = {
    unescaped.replaceAll("\"", "\"\"")
  }
}
