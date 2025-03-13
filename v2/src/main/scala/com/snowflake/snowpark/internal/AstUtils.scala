package com.snowflake.snowpark.internal

import com.google.protobuf.ByteString
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.proto.ast.Expr.{INT64_VAL_FIELD_NUMBER, Variant, scalaDescriptor}
import com.snowflake.snowpark.proto.ast._

import java.math.{BigDecimal => JavaBigDecimal}
import scala.collection.mutable

object AstUtils {
  private val filenames: mutable.Map[String, Int] = mutable.Map.empty
  private val internedValueTable: mutable.Map[Int, String] = mutable.Map.empty

  private[snowpark] def getInternedValueTable: Option[InternedValueTable] = this.synchronized {
    if (internedValueTable.isEmpty) {
      None
    } else {
      Some(InternedValueTable(stringValues = internedValueTable.toMap))
    }
  }

  private[snowpark] def getFileId(filename: String): Int = this.synchronized {
    // return file id if it has been seen before,
    // otherwise set the size of map to be the id and add it to the map.
    filenames.getOrElseUpdate(
      filename, {
        val index = filenames.size
        internedValueTable.put(index, filename)
        index
      })
  }

  lazy val language: Language = Language.LanguageTypeMapper.toCustom(
    LanguageMessage.of(LanguageMessage.SealedValue.ScalaLanguage(
      ScalaLanguage(version = Some(Version(label = "3.0.0-M1", major = 1, minor = 0, patch = 0))))))

  private[snowpark] def createSroPosition(srcPositionInfo: SrcPositionInfo): Option[SrcPosition] = {
    if (srcPositionInfo != null) {
      Some(
        SrcPosition(
          file = getFileId(srcPositionInfo.filename),
          startLine = srcPositionInfo.line,
          startColumn = srcPositionInfo.column))
    } else None
  }

  private[snowpark] def createExpr(value: Any, srcPositionInfo: SrcPositionInfo): Expr = {
    toExpr(value, createSroPosition(srcPositionInfo))
  }

  private def toExpr(value: Any, srcPosition: Option[SrcPosition] = None): Expr = value match {
    case null => toExpr(Variant.NullVal(NullVal(src = srcPosition)))
    case expr: Expr => expr
    case variant: Variant => Expr(variant)
    case column: Column => column.expr
    case str @ (_: String | _: Char) =>
      toExpr(Variant.StringVal(StringVal(v = str.toString, src = srcPosition)))
    case i: Int => toExpr(i.toLong, srcPosition)
    case b: Byte => toExpr(b.toLong, srcPosition)
    case s: Short => toExpr(s.toLong, srcPosition)
    case l: Long => toExpr(Variant.Int64Val(Int64Val(v = l, src = srcPosition)))
    case f: Float => toExpr(f.toDouble, srcPosition)
    case d: Double => toExpr(Variant.Float64Val(Float64Val(v = d, src = srcPosition)))
    case bool: Boolean => toExpr(Variant.BoolVal(BoolVal(v = bool, src = srcPosition)))
    case decimal: BigDecimal => toExpr(decimal.bigDecimal, srcPosition)
    case decimal: JavaBigDecimal =>
      toExpr(
        Variant.BigDecimalVal(
          BigDecimalVal(
            scale = decimal.scale(),
            unscaledValue = ByteString.copyFrom(decimal.unscaledValue().toByteArray),
            src = srcPosition)))
    case bytes: Array[Byte] =>
      toExpr(Variant.BinaryVal(BinaryVal(v = ByteString.copyFrom(bytes), src = srcPosition)))
    // todo: SNOW-1961939 add support for timestamp and date literals
    //    case i: Instant => null
    //    case t: Timestamp => null
    //    case d: Date => null
    //    case ld: LocalDate => null
    case _ => throw new IllegalArgumentException(s"Unsupported value type: ${value.getClass}")
  }

}
