package com.snowflake.code_verification

import com.snowflake.snowpark.{CodeVerification, DataFrame}
import org.scalatest.FunSuite

// verify API Java and Scala API contain same functions
@CodeVerification
class JavaScalaAPISuite extends FunSuite {
  private val scalaCaseClassFunctions = Set(
    "apply",
    "copy",
    "canEqual",
    "productElement",
    "andThen",
    "productPrefix",
    "productIterator",
    "compose",
    "productArity",
    "unapply",
    "tupled",
    "curried"
  )

  // used to get list of Scala Seq functions
  class FakeSeq extends Seq[String] {
    override def length: Int = 1

    override def apply(idx: Int): String = ""

    override def iterator: Iterator[String] = null
  }

  private val scalaSeqFunctions: Set[String] =
    ClassUtils.getAllPublicFunctionNames(classOf[FakeSeq]).toSet

  test("functions") {
    import com.snowflake.snowpark_java.{Functions => JavaFunctions}
    import com.snowflake.snowpark.{functions => ScalaFunctions}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaFunctions],
        ScalaFunctions.getClass,
        class2Only = Set(
          "column", // Java API has "col", Scala API has both "col" and "column"
          "callBuiltin", // Java API has "callUDF", Scala API has both "callBuiltin" and "callUDF"
          "typedLit", // Scala API only, Java API has lit
          "builtin"
        ),
        class1To2NameMap = Map("chr" -> "char")
      )
    )
  }

  test("AsyncJob") {
    import com.snowflake.snowpark_java.{AsyncJob => JavaAsyncJob}
    import com.snowflake.snowpark.{AsyncJob => ScalaAsyncJob}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaAsyncJob], classOf[ScalaAsyncJob]))
  }

  test("Column") {
    import com.snowflake.snowpark_java.{Column => JavaColumn}
    import com.snowflake.snowpark.{Column => ScalaColumn}
    // it doesn't work with scala operator overloading
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaColumn],
        classOf[ScalaColumn],
        class1Only = Set(
          "unary_not", // Scala API has "unary_!"
          "unary_minus" // Scala API has "unary_-"
        ),
        class2Only = Set(
          "name" // Java API has "alias"
        ) ++ scalaCaseClassFunctions,
        class1To2NameMap = Map("subField" -> "apply")
      )
    )
  }

  test("CaseExpr") {
    import com.snowflake.snowpark_java.{CaseExpr => JavaCaseExpr}
    import com.snowflake.snowpark.{CaseExpr => ScalaCaseExpr}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaCaseExpr],
        classOf[ScalaCaseExpr],
        // Java API has "otherwise", Scala API has both "otherwise" and "else"
        class2Only = Set("else")
      )
    )
  }

  test("DataFrame") {
    import com.snowflake.snowpark_java.{DataFrame => JavaDataFrame}
    import com.snowflake.snowpark.{DataFrame => ScalaDataFrame}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrame],
        classOf[ScalaDataFrame],
        class2Only = Set(
          // package private functions
          "getUnaliased",
          "methodChainCache",
          "buildMethodChain",
          "generatePrefix"
        ) ++ scalaCaseClassFunctions
      )
    )
  }

  test("CopyableDataFrame") {
    import com.snowflake.snowpark_java.{CopyableDataFrame => JavaCopyableDataFrame}
    import com.snowflake.snowpark.{CopyableDataFrame => ScalaCopyableDataFrame}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaCopyableDataFrame],
        classOf[ScalaCopyableDataFrame],
        // package private
        class2Only = Set("getCopyDataFrame")
      )
    )
  }

  test("CopyableDataFrameAsyncActor") {
    import com.snowflake.snowpark.{CopyableDataFrameAsyncActor => ScalaCopyableDataFrameAsyncActor}
    import com.snowflake.snowpark_java.{
      CopyableDataFrameAsyncActor => JavaCopyableDataFrameAsyncActor
    }
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaCopyableDataFrameAsyncActor],
        classOf[ScalaCopyableDataFrameAsyncActor]
      )
    )
  }

  test("DataFrameAsyncActor") {
    import com.snowflake.snowpark_java.{DataFrameAsyncActor => JavaDataFrameAsyncActor}
    import com.snowflake.snowpark.{DataFrameAsyncActor => ScalaDataFrameAsyncActor}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrameAsyncActor],
        classOf[ScalaDataFrameAsyncActor]
      )
    )
  }

  test("DataFrameNaFunctions") {
    import com.snowflake.snowpark_java.{DataFrameNaFunctions => JavaDataFrameNaFunctions}
    import com.snowflake.snowpark.{DataFrameNaFunctions => ScalaDataFrameNaFunctions}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrameNaFunctions],
        classOf[ScalaDataFrameNaFunctions]
      )
    )
  }

  test("DataFrameReader") {
    import com.snowflake.snowpark_java.{DataFrameReader => JavaDataFrameReader}
    import com.snowflake.snowpark.{DataFrameReader => ScalaDataFrameReader}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaDataFrameReader], classOf[ScalaDataFrameReader])
    )
  }

  test("DataFrameStatFunctions") {
    import com.snowflake.snowpark_java.{DataFrameStatFunctions => JavaDataFrameStatFunctions}
    import com.snowflake.snowpark.{DataFrameStatFunctions => ScalaDataFrameStatFunctions}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrameStatFunctions],
        classOf[ScalaDataFrameStatFunctions]
      )
    )
  }

  test("DataFrameWriter") {
    import com.snowflake.snowpark_java.{DataFrameWriter => JavaDataFrameWriter}
    import com.snowflake.snowpark.{DataFrameWriter => ScalaDataFrameWriter}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrameWriter],
        classOf[ScalaDataFrameWriter],
        // package private
        class2Only = Set("getWritePlan")
      )
    )
  }

  test("DataFrameWriterAsyncActor") {
    import com.snowflake.snowpark_java.{DataFrameWriterAsyncActor => JavaDataFrameWriterAsyncActor}
    import com.snowflake.snowpark.{DataFrameWriterAsyncActor => ScalaDataFrameWriterAsyncActor}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDataFrameWriterAsyncActor],
        classOf[ScalaDataFrameWriterAsyncActor]
      )
    )
  }

  test("DeleteResult") {
    import com.snowflake.snowpark_java.{DeleteResult => JavaDeleteResult}
    import com.snowflake.snowpark.{DeleteResult => ScalaDeleteResult}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDeleteResult],
        classOf[ScalaDeleteResult],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map("getRowsDeleted" -> "rowsDeleted")
      )
    )
  }

  test("FileOperation") {
    import com.snowflake.snowpark_java.{FileOperation => JavaFileOperation}
    import com.snowflake.snowpark.{FileOperation => ScalaFileOperation}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaFileOperation], classOf[ScalaFileOperation])
    )
  }

  test("GetResult") {
    import com.snowflake.snowpark_java.{GetResult => JavaGetResult}
    import com.snowflake.snowpark.{GetResult => ScalaGetResult}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaGetResult],
        classOf[ScalaGetResult],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map(
          "getEncryption" -> "encryption",
          "getStatus" -> "status",
          "getSizeBytes" -> "sizeBytes",
          "getMessage" -> "message",
          "getFileName" -> "fileName"
        )
      )
    )
  }

  test("GroupingSets") {
    import com.snowflake.snowpark_java.{GroupingSets => JavaGroupingSets}
    import com.snowflake.snowpark.{GroupingSets => ScalaGroupingSets}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaGroupingSets],
        classOf[ScalaGroupingSets],
        class1Only = Set(),
        class2Only = Set("sets", "toExpression") ++ scalaCaseClassFunctions,
        class1To2NameMap = Map("create" -> "apply")
      )
    )
  }

  test("HasCachedResult") {
    import com.snowflake.snowpark_java.{HasCachedResult => JavaHasCachedResult}
    import com.snowflake.snowpark.{HasCachedResult => ScalaHasCachedResult}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaHasCachedResult], classOf[ScalaHasCachedResult])
    )
  }

  test("MatchedClauseBuilder") {
    import com.snowflake.snowpark_java.{MatchedClauseBuilder => JavaMatchedClauseBuilder}
    import com.snowflake.snowpark.{MatchedClauseBuilder => ScalaMatchedClauseBuilder}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaMatchedClauseBuilder],
        classOf[ScalaMatchedClauseBuilder],
        // scala api has update[T]
        class1Only = Set("updateColumn")
      )
    )
  }

  test("MergeBuilder") {
    import com.snowflake.snowpark_java.{MergeBuilder => JavaMergeBuilder}
    import com.snowflake.snowpark.{MergeBuilder => ScalaMergeBuilder}
    assert(
      ClassUtils.containsSameFunctionNames(classOf[JavaMergeBuilder], classOf[ScalaMergeBuilder])
    )
  }

  test("MergeResult") {
    import com.snowflake.snowpark_java.{MergeResult => JavaMergeResult}
    import com.snowflake.snowpark.{MergeResult => ScalaMergeResult}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaMergeResult],
        classOf[ScalaMergeResult],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map(
          "getRowsInserted" -> "rowsInserted",
          "getRowsUpdated" -> "rowsUpdated",
          "getRowsDeleted" -> "rowsDeleted"
        )
      )
    )
  }

  test("NotMatchedClauseBuilder") {
    import com.snowflake.snowpark_java.{NotMatchedClauseBuilder => JavaNotMatchedClauseBuilder}
    import com.snowflake.snowpark.{NotMatchedClauseBuilder => ScalaNotMatchedClauseBuilder}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaNotMatchedClauseBuilder],
        classOf[ScalaNotMatchedClauseBuilder],
        class1Only = Set("insertRow")
      )
    )
  }

  test("PutResult") {
    import com.snowflake.snowpark_java.{PutResult => JavaPutResult}
    import com.snowflake.snowpark.{PutResult => ScalaPutResult}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaPutResult],
        classOf[ScalaPutResult],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map(
          "getTargetCompression" -> "targetCompression",
          "getEncryption" -> "encryption",
          "getStatus" -> "status",
          "getTargetFileName" -> "targetFileName",
          "getMessage" -> "message",
          "getSourceFileName" -> "sourceFileName",
          "getSourceCompression" -> "sourceCompression",
          "getTargetSizeBytes" -> "targetSizeBytes",
          "getSourceSizeBytes" -> "sourceSizeBytes"
        )
      )
    )
  }

  test("RelationalGroupedDataFrame") {
    import com.snowflake.snowpark_java.{
      RelationalGroupedDataFrame => JavaRelationalGroupedDataFrame
    }
    import com.snowflake.snowpark.{RelationalGroupedDataFrame => ScalaRelationalGroupedDataFrame}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaRelationalGroupedDataFrame],
        classOf[ScalaRelationalGroupedDataFrame]
      )
    )
  }

  test("Row") {
    import com.snowflake.snowpark_java.{Row => JavaRow}
    import com.snowflake.snowpark.{Row => ScalaRow}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaRow],
        classOf[ScalaRow],
        class1Only = Set(),
        class2Only = Set(
          "fromArray",
          "fromSeq",
          "length" // Java API has "size"
        ) ++ scalaCaseClassFunctions,
        class1To2NameMap = Map(
          "toList" -> "toSeq",
          "create" -> "apply",
          "getListOfVariant" -> "getSeqOfVariant",
          "getList" -> "getSeq"
        )
      )
    )
  }

  // Java SaveMode is an Enum,
  // but Scala SaveMode is a list of Object, skip

  test("Session") {
    import com.snowflake.snowpark_java.{Session => JavaSession}
    import com.snowflake.snowpark.{Session => ScalaSession}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaSession],
        classOf[ScalaSession],
        class1Only = Set(),
        class2Only = Set(
          "storedProcedure", // todo in snow-683655
          "sproc", // todo in snow-683653
          "getDependenciesAsJavaSet", // Java API renamed to "getDependencies"
          "implicits"
        )
      )
    )
  }

  test("SessionBuilder") {
    import com.snowflake.snowpark_java.{SessionBuilder => JavaSessionBuilder}
    import com.snowflake.snowpark.Session.{SessionBuilder => ScalaSessionBuilder}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaSessionBuilder], classOf[ScalaSessionBuilder])
    )
  }

  test("TableFunction") {
    import com.snowflake.snowpark_java.{TableFunction => JavaTableFunction}
    import com.snowflake.snowpark.{TableFunction => ScalaTableFunction}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaTableFunction],
        classOf[ScalaTableFunction],
        class1Only = Set("call"), // `call` in Scala is `apply`
        class2Only = Set("funcName") ++ scalaCaseClassFunctions
      )
    )
  }

  test("TableFunctions") {
    import com.snowflake.snowpark_java.{TableFunctions => JavaTableFunctions}
    import com.snowflake.snowpark.{tableFunctions => ScalaTableFunctions}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaTableFunctions], ScalaTableFunctions.getClass)
    )
  }

  test("TypedAsyncJob") {
    import com.snowflake.snowpark_java.{TypedAsyncJob => JavaTypedAsyncJob}
    import com.snowflake.snowpark.{TypedAsyncJob => ScalaTypedAsyncJob}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaTypedAsyncJob[_]], classOf[ScalaTypedAsyncJob[_]])
    )
  }

  test("UDFRegistration") {
    import com.snowflake.snowpark_java.{UDFRegistration => JavaUDFRegistration}
    import com.snowflake.snowpark.{UDFRegistration => ScalaUDFRegistration}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaUDFRegistration], classOf[ScalaUDFRegistration])
    )
  }

  test("Updatable") {
    import com.snowflake.snowpark_java.{Updatable => JavaUpdatable}
    import com.snowflake.snowpark.{Updatable => ScalaUpdatable}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaUpdatable],
        classOf[ScalaUpdatable],
        class1Only = Set("updateColumn")
      )
    )
  }

  test("UpdatableAsyncActor") {
    import com.snowflake.snowpark_java.{UpdatableAsyncActor => JavaUpdatableAsyncActor}
    import com.snowflake.snowpark.{UpdatableAsyncActor => ScalaUpdatableAsyncActor}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaUpdatableAsyncActor],
        classOf[ScalaUpdatableAsyncActor],
        class1Only = Set("updateColumn")
      )
    )
  }

  test("UpdateResult") {
    import com.snowflake.snowpark_java.{UpdateResult => JavaUpdateResult}
    import com.snowflake.snowpark.{UpdateResult => ScalaUpdateResult}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaUpdateResult],
        classOf[ScalaUpdateResult],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map(
          "getRowsUpdated" -> "rowsUpdated",
          "getMultiJoinedRowsUpdated" -> "multiJoinedRowsUpdated"
        )
      )
    )
  }

  test("UserDefinedFunction") {
    import com.snowflake.snowpark_java.{UserDefinedFunction => JavaUserDefinedFunction}
    import com.snowflake.snowpark.{UserDefinedFunction => ScalaUserDefinedFunction}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaUserDefinedFunction],
        classOf[ScalaUserDefinedFunction],
        class1Only = Set(),
        class2Only = Set("f", "returnType", "name", "inputTypes", "withName") ++
          scalaCaseClassFunctions
      )
    )
  }

  test("Windows") {
    import com.snowflake.snowpark_java.{Window => JavaWindow}
    import com.snowflake.snowpark.{Window => ScalaWindow}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaWindow], ScalaWindow.getClass))
  }

  test("WindowSpec") {
    import com.snowflake.snowpark_java.{WindowSpec => JavaWindowSpec}
    import com.snowflake.snowpark.{WindowSpec => ScalaWindowSpec}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaWindowSpec], classOf[ScalaWindowSpec]))
  }

  // types
  test("ArrayType") {
    import com.snowflake.snowpark_java.types.{ArrayType => JavaArrayType}
    import com.snowflake.snowpark.types.{ArrayType => ScalaArrayType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaArrayType],
        classOf[ScalaArrayType],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions,
        class1To2NameMap = Map("getElementType" -> "elementType")
      )
    )
  }

  test("BinaryType") {
    import com.snowflake.snowpark_java.types.{BinaryType => JavaBinaryType}
    import com.snowflake.snowpark.types.{BinaryType => ScalaBinaryType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaBinaryType],
        ScalaBinaryType.getClass,
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions
      )
    )
  }

  test("BooleanType") {
    import com.snowflake.snowpark_java.types.{BooleanType => JavaBooleanType}
    import com.snowflake.snowpark.types.{BooleanType => ScalaBooleanType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaBooleanType],
        ScalaBooleanType.getClass,
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions
      )
    )
  }

  test("ByteType") {
    import com.snowflake.snowpark_java.types.{ByteType => JavaByteType}
    import com.snowflake.snowpark.types.{ByteType => ScalaByteType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaByteType],
        ScalaByteType.getClass,
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions
      )
    )
  }

  test("ColumnIdentifier") {
    import com.snowflake.snowpark_java.types.{ColumnIdentifier => JavaColumnIdentifier}
    import com.snowflake.snowpark.types.{ColumnIdentifier => ScalaColumnIdentifier}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaColumnIdentifier],
        classOf[ScalaColumnIdentifier],
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions
      )
    )
  }

  test("DateType") {
    import com.snowflake.snowpark_java.types.{DateType => JavaDateType}
    import com.snowflake.snowpark.types.{DateType => ScalaDateType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDateType],
        ScalaDateType.getClass,
        class1Only = Set(),
        class2Only = scalaCaseClassFunctions
      )
    )
  }

  test("DecimalType") {
    import com.snowflake.snowpark_java.types.{DecimalType => JavaDecimalType}
    import com.snowflake.snowpark.types.{DecimalType => ScalaDecimalType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaDecimalType],
        ScalaDecimalType.getClass,
        class1Only = Set("getPrecision", "getScale"),
        class2Only = Set("MAX_SCALE", "MAX_PRECISION") ++
          scalaCaseClassFunctions
      )
    )
  }

  test("DoubleType") {
    import com.snowflake.snowpark_java.types.{DoubleType => JavaDoubleType}
    import com.snowflake.snowpark.types.{DoubleType => ScalaDoubleType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaDoubleType], ScalaDoubleType.getClass))
  }

  test("FloatType") {
    import com.snowflake.snowpark_java.types.{FloatType => JavaFloatType}
    import com.snowflake.snowpark.types.{FloatType => ScalaFloatType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaFloatType], ScalaFloatType.getClass))
  }

  test("Geography") {
    import com.snowflake.snowpark_java.types.{Geography => JavaGeography}
    import com.snowflake.snowpark.types.{Geography => ScalaGeograhy}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaGeography],
        classOf[ScalaGeograhy],
        class2Only = Set("getString")
      )
    )
  }

  test("GeographyType") {
    import com.snowflake.snowpark_java.types.{GeographyType => JavaGeographyType}
    import com.snowflake.snowpark.types.{GeographyType => ScalaGeograhyType}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaGeographyType], ScalaGeograhyType.getClass)
    )
  }

  test("Geometry") {
    import com.snowflake.snowpark_java.types.{Geometry => JavaGeometry}
    import com.snowflake.snowpark.types.{Geometry => ScalaGeometry}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaGeometry], classOf[ScalaGeometry]))
  }

  test("GeometryType") {
    import com.snowflake.snowpark_java.types.{GeometryType => JavaGeometryType}
    import com.snowflake.snowpark.types.{GeometryType => ScalaGeometryType}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaGeometryType], ScalaGeometryType.getClass)
    )
  }

  test("IntegerType") {
    import com.snowflake.snowpark_java.types.{IntegerType => JavaIntegerType}
    import com.snowflake.snowpark.types.{IntegerType => ScalaIntegerType}
    assert(
      ClassUtils.containsSameFunctionNames(classOf[JavaIntegerType], ScalaIntegerType.getClass)
    )
  }

  test("LongType") {
    import com.snowflake.snowpark_java.types.{LongType => JavaLongType}
    import com.snowflake.snowpark.types.{LongType => ScalaLongType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaLongType], ScalaLongType.getClass))
  }

  test("MapType") {
    import com.snowflake.snowpark_java.types.{MapType => JavaMapType}
    import com.snowflake.snowpark.types.{MapType => ScalaMapType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaMapType],
        ScalaMapType.getClass,
        class1Only = Set("getValueType", "getKeyType"),
        class2Only = Set("unapply")
      )
    )
  }

  test("ShortType") {
    import com.snowflake.snowpark_java.types.{ShortType => JavaShortType}
    import com.snowflake.snowpark.types.{ShortType => ScalaShortType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaShortType], ScalaShortType.getClass))
  }

  test("StringType") {
    import com.snowflake.snowpark_java.types.{StringType => JavaStringType}
    import com.snowflake.snowpark.types.{StringType => ScalaStringType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaStringType], ScalaStringType.getClass))
  }

  test("TimestampType") {
    import com.snowflake.snowpark_java.types.{TimestampType => JavaTimestampType}
    import com.snowflake.snowpark.types.{TimestampType => ScalaTimestampType}
    assert(
      ClassUtils
        .containsSameFunctionNames(classOf[JavaTimestampType], ScalaTimestampType.getClass)
    )
  }

  test("TimeType") {
    import com.snowflake.snowpark_java.types.{TimeType => JavaTimeType}
    import com.snowflake.snowpark.types.{TimeType => ScalaTimeType}
    assert(ClassUtils.containsSameFunctionNames(classOf[JavaTimeType], ScalaTimeType.getClass))
  }

  test("VariantType") {
    import com.snowflake.snowpark_java.types.{VariantType => JavaVariantType}
    import com.snowflake.snowpark.types.{VariantType => ScalaVariantType}
    assert(
      ClassUtils.containsSameFunctionNames(classOf[JavaVariantType], ScalaVariantType.getClass)
    )
  }

  test("Variant") {
    import com.snowflake.snowpark_java.types.{Variant => JavaVariant}
    import com.snowflake.snowpark.types.{Variant => ScalaVariant}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaVariant],
        classOf[ScalaVariant],
        class1Only = Set(),
        class2Only = Set("dataType", "value"),
        class1To2NameMap = Map("asBigInteger" -> "asBigInt", "asList" -> "asSeq")
      )
    )
  }

  test("StructField") {
    import com.snowflake.snowpark_java.types.{StructField => JavaStructField}
    import com.snowflake.snowpark.types.{StructField => ScalaStructField}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaStructField],
        classOf[ScalaStructField],
        class1Only = Set(),
        class2Only = Set("treeString") ++ scalaCaseClassFunctions
      )
    )
  }

  test("StructType") {
    import com.snowflake.snowpark_java.types.{StructType => JavaStructType}
    import com.snowflake.snowpark.types.{StructType => ScalaStructType}
    assert(
      ClassUtils.containsSameFunctionNames(
        classOf[JavaStructType],
        classOf[ScalaStructType],
        class1Only = Set(
          // Java Iterable
          "forEach",
          "get",
          "spliterator"
        ),
        class2Only = Set("fields") ++ scalaSeqFunctions ++
          scalaCaseClassFunctions,
        class1To2NameMap = Map("create" -> "apply")
      )
    )
  }

}
