package com.snowflake.snowpark.types

import com.snowflake.snowpark.internal.analyzer.Attribute
import com.snowflake.snowpark.internal.analyzer

/**
 * StructType data type, represents table schema.
 * @since 0.1.0
 */
object StructType {

  private[snowpark] def fromAttributes(attrs: Seq[Attribute]): StructType =
    StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable)))

  /**
   * Clones the given [[StructType]] object.
   * @since 0.1.0
   */
  def apply(other: StructType): StructType = StructType(other.fields)

  /**
   * Creates a [[StructType]] object based on the given Seq of [[StructField]]
   * @since 0.1.0
   */
  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)

  /**
   * Creates a [[StructType]] object based on the given [[StructField]]
   * @since 0.7.0
   */
  def apply(field: StructField, remaining: StructField*): StructType = apply(field +: remaining)
}

/**
 * StructType data type, represents table schema.
 * @constructor Creates a new [[StructType]] object based on the given array of [[StructField]].
 * @since 0.1.0
 */
case class StructType(fields: Array[StructField] = Array())
    extends DataType
    with Seq[StructField] {

  /**
   * Returns the total number of [[StructField]]
   * @since 0.1.0
   */
  override def length: Int = fields.length

  /**
   * Converts this object to Iterator.
   * @since 0.1.0
   */
  override def iterator: Iterator[StructField] = fields.toIterator

  /**
   * Returns the corresponding [[StructField]] of the given index.
   * @since 0.1.0
   */
  override def apply(idx: Int): StructField = fields(idx)

  /**
   * Returns a String values to represent this object info.
   * @since 0.1.0
   */
  override def toString: String =
    s"StructType[${fields.map(_.toString).mkString(", ")}]"

  /**
   * Appends a new [[StructField]] to the end of this object.
   * @since 0.1.0
   */
  def add(field: StructField): StructType = StructType(fields :+ field)

  /**
   * Appends a new [[StructField]] to the end of this object.
   * @since 0.1.0
   */
  def add(name: String, dataType: DataType, nullable: Boolean = true): StructType =
    add(StructField(name, dataType, nullable))

  /**
   * (Scala API Only) Returns a Seq of the name of [[StructField]].
   * @since 0.1.0
   */
  def names: Seq[String] = fields.map(_.name)

  /**
   * Returns the corresponding [[StructField]] object of the given name.
   * @since 0.1.0
   */
  def nameToField(name: String): Option[StructField] =
    fields.find(_.columnIdentifier.quotedName == analyzer.quoteName(name))

  /**
   * Return the corresponding [[StructField]] object of the given name.
   * @since 0.1.0
   */
  def apply(name: String): StructField =
    nameToField(name).getOrElse(
      throw new IllegalArgumentException(s"$name does not exits. Names: ${names.mkString(", ")}"))

  protected[snowpark] def toAttributes: Seq[Attribute] = {
    /*
     * When user provided schema is used in a SnowflakePlan, we have to
     * to invoke quoteName on the column names to align with the
     * case-insensitive(unless quoted) identifier naming that snowflake follows.
     */
    map(f => Attribute(f.columnIdentifier.quotedName, f.dataType, f.nullable))
  }

  /**
   * Prints the StructType content in a tree structure diagram.
   * @since 0.9.0
   */
  def printTreeString(): Unit =
    // scalastyle:off
    println(treeString(0))
  // scalastyle:on

  private[snowpark] def treeString(layer: Int): String =
    (if (layer == 0) "root\n" else "") + fields.map(_.treeString(layer)).mkString

}

/**
 * Constructors and Util functions of [[StructField]]
 * @since 0.1.0
 */
object StructField {

  /**
   * Creates a [[StructField]]
   *
   * @since 0.1.0
   */
  def apply(name: String, dataType: DataType, nullable: Boolean): StructField =
    StructField(ColumnIdentifier(name), dataType, nullable)

  /**
   * Creates a [[StructField]]
   *
   * @since 0.1.0
   */
  def apply(name: String, dataType: DataType): StructField =
    StructField(ColumnIdentifier(name), dataType)
}

/**
 * Represents the content of [[StructType]].
 * @since 0.1.0
 */
case class StructField(
    columnIdentifier: ColumnIdentifier,
    dataType: DataType,
    nullable: Boolean = true) {

  /**
   * Returns the column name.
   * @since 0.1.0
   */
  val name: String = columnIdentifier.name

  /**
   * Returns a String values to represent this object info.
   * @since 0.1.0
   */
  override def toString: String = s"StructField($name, $dataType, Nullable = $nullable)"

  private[types] def treeString(layer: Int): String = {
    val prepended: String = (1 to (1 + 2 * layer)).map(x => " ").mkString + "|--"
    val body: String = s"$name: ${dataType.typeName} (nullable = $nullable)\n" +
      (dataType match {
        case st: StructType => st.treeString(layer + 1)
        case _ => ""
      })

    prepended + body
  }
}

/**
 *  Constructors and Util functions of ColumnIdentifier
 *  @since 0.1.0
 */
object ColumnIdentifier {

  /**
   * Creates a [[ColumnIdentifier]] object for the giving column name.
   * Identifier Requirement can be found from
   * https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
   * @since 0.1.0
   */
  def apply(name: String): ColumnIdentifier =
    new ColumnIdentifier(analyzer.quoteName(name))

  /**
   * Removes the unnecessary quotes from name
   *
   * Remove quotes if name starts with _A-Z and only contains _0-9A-Z$, or
   * starts with $ and follows by digits
   * @since 0.1.0
   */
  private def stripUnnecessaryQuotes(str: String): String = {
    val removeQuote = "^\"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))\"$".r
    str match {
      case removeQuote(n, _, _) => n
      case n => n
    }
  }
}

/**
 * Represents Column Identifier
 * @since 0.1.0
 */
class ColumnIdentifier private (normalizedName: String) {

  /**
   * Returns the name of column.
   * Name format:
   * 1. if the name quoted.
   *   a. starts with _A-Z and follows by _A-Z0-9$: remove quotes
   *   b. starts with $ and follows by digits: remove quotes
   *   c. otherwise, do nothing
   * 2. if not quoted.
   *   a. starts with _a-zA-Z and follows by _a-zA-Z0-9$, upper case all letters.
   *   b. starts with $ and follows by digits, do nothing
   *   c. otherwise, quote name
   *
   * More details can be found from
   * https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
   * @since 0.1.0
   */
  val name: String = ColumnIdentifier.stripUnnecessaryQuotes(normalizedName)

  /**
   * Returns the quoted name of this column
   * Name Format:
   * 1. if quoted, do nothing
   * 2. if not quoted.
   *   a. starts with _a-zA-Z and follows by _a-zA-Z0-9$, upper case all letters
   *     and then quote
   *   b. otherwise, quote name
   *
   * It is same as [[name]], but quotes always added.
   * It is always safe to do String comparisons between quoted column names
   * @since 0.1.0
   */
  def quotedName: String = normalizedName

  /**
   * Returns a copy of this [[ColumnIdentifier]].
   * @since 0.1.0
   */
  override def clone(): AnyRef = new ColumnIdentifier(normalizedName)

  /**
   * Returns the hashCode of this [[ColumnIdentifier]].
   * @since 0.1.0
   */
  override def hashCode(): Int = normalizedName.hashCode

  /**
   * Compares this [[ColumnIdentifier]] with the giving one, returns true if these
   * two are equivalent, otherwise, returns false.
   * @since 0.1.0
   */
  override def equals(obj: Any): Boolean =
    obj match {
      case other: ColumnIdentifier => normalizedName == other.quotedName
      case _ => false
    }

  /**
   * Returns the column name. Alias of [[name]]
   * @since 0.1.0
   */
  override def toString: String = name
}
