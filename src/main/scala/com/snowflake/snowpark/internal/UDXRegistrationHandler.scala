package com.snowflake.snowpark.internal

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  File,
  PipedInputStream,
  PipedOutputStream
}
import java.util.jar.JarOutputStream
import com.snowflake.snowpark.internal.Utils.{TempObjectType, randomNameForTempObject}
import com.snowflake.snowpark._
import com.snowflake.snowpark.internal.analyzer.TempType
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.udtf._
import com.snowflake.snowpark_java.internal._
import com.snowflake.snowpark_java.types.{StructType => JavaStructType}
import com.snowflake.snowpark_java.udtf._
import com.snowflake.snowpark_java.sproc._
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.reflect.internal.util.AbstractFileClassLoader
import scala.reflect.io.{AbstractFile, VirtualDirectory}
import scala.util.Random

class UDXRegistrationHandler(session: Session) extends Logging {
  // Class name for generated Java code
  private val className = "SnowUDF"
  // Method name for generated Java code
  private val methodName = "compute"
  private val udtfClassName = "SnowparkGeneratedUDTF"
  private val jarBuilder = new FatJarBuilder()

  private val javaUdtfDefaultStructType =
    com.snowflake.snowpark_java.types.StructType.create()

  private[snowpark] def addSnowparkJarToDeps(): Unit = {
    UDFClassPath.snowparkJar.location match {
      case Some(path) =>
        session.addDependency(path)
        session.snowparkJarInDeps = true
        logInfo(s"Automatically added $path to session dependencies.")
      case _ =>
    }
    for (jar <- UDFClassPath.jacksonJarSeq) {
      jar.location match {
        case Some(path) =>
          session.addDependency(path)
          logInfo(s"Automatically added $path to session dependencies.")
        case _ =>
      }
    }
  }

  // If version is supported by server, create udf with server packages
  // Only upload jar when version is not supported by server packages
  if (session.isVersionSupportedByServerPackages) {
    session.addPackage(Utils.clientPackageName)
  } else if (!session.snowparkJarInDeps) {
    addSnowparkJarToDeps()
  }

  private def retryAfterFixingClassPath[T](func: => Unit): Unit = {
    try {
      func
    } catch {
      case e: SnowflakeSQLException =>
        val msg = e.getMessage
        if (msg.contains("NoClassDefFoundError: com/snowflake/snowpark/") ||
            msg.contains("error: package com.snowflake.snowpark.internal does not exist")) {
          logInfo("Snowpark jar is missing in imports, Retrying after uploading the jar")
          addSnowparkJarToDeps()
          func
        } else {
          throw e
        }
    }
  }

  private def wrapUploadTimeoutException[T](func: => Seq[String]): Seq[String] = {
    try {
      func
    } catch {
      case _: TimeoutException =>
        throw ErrorMessage.MISC_REQUEST_TIMEOUT(
          "UDF jar uploading",
          session.requestTimeoutInSeconds)
    }
  }

  private def getAndValidateFunctionName(name: Option[String]) = {
    val funcName = name.getOrElse(
      session.getFullyQualifiedCurrentSchema + "." + randomNameForTempObject(
        TempObjectType.Function))
    Utils.validateObjectName(funcName)
    funcName
  }

  private[snowpark] def registerSP(
      name: Option[String],
      sp: StoredProcedure,
      stageLocation: Option[String],
      isCallerMode: Boolean): StoredProcedure = {
    val spName = getAndValidateFunctionName(name)
    // Clean up closure
    cleanupClosure(sp.sp)
    // Generate SP inline java code
    val inputArgs = sp.inputTypes.zipWithIndex.map {
      case (schema, i) => UdfColumn(schema, s"arg$i")
    }
    val (code, funcBytesMap) = generateJavaSPCode(sp.sp, sp.returnType, inputArgs)
    val needCleanupFiles = Utils.createConcurrentSet[String]()
    withUploadFailureCleanup(stageLocation, needCleanupFiles) {
      retryAfterFixingClassPath {
        val (allImports, targetJarStageLocation) =
          uploadDependencies(spName, sp.sp, needCleanupFiles, funcBytesMap, stageLocation)
        createJavaSP(
          sp.returnType.dataType,
          inputArgs,
          spName,
          allImports.map(i => s"'$i'").mkString(","),
          stageLocation.isEmpty,
          code,
          targetJarStageLocation,
          isCallerMode)
      }
    }
    sp.withName(spName)
  }

  private[snowpark] def registerUDF(
      name: Option[String],
      udf: UserDefinedFunction,
      // if stageLocation is none, this udf will be temporary udf
      stageLocation: Option[String]): UserDefinedFunction = {
    val udfName = getAndValidateFunctionName(name)
    // Clean up closure
    cleanupClosure(udf.f)
    // Generate UDF inline java code
    val argNames = (1 to udf.inputTypes.length).map(i => s"arg$i")
    val inputArgs = udf.inputTypes.zip(argNames).map(entry => UdfColumn(entry._1, entry._2))
    val (code, funcBytesMap) = generateJavaUDFCode(udf.f, udf.returnType, inputArgs)
    // upload dependencies and create UDF
    val needCleanupFiles = Utils.createConcurrentSet[String]()
    withUploadFailureCleanup(stageLocation, needCleanupFiles) {
      retryAfterFixingClassPath {
        val (allImports, targetJarStageLocation) =
          uploadDependencies(udfName, udf.f, needCleanupFiles, funcBytesMap, stageLocation)
        // create UDF function
        createJavaUDF(
          udf.returnType.dataType,
          inputArgs,
          udfName,
          allImports.map(i => s"'$i'").mkString(","),
          stageLocation.isEmpty,
          code,
          targetJarStageLocation)
      }
    }
    udf.withName(udfName)
  }

  // Internal function to register UDTF.
  private[snowpark] def registerUDTF(
      name: Option[String],
      udtf: UDTF,
      // if stageLocation is none, this udf will be temporary udtf
      stageLocation: Option[String] = None): TableFunction = {
    ScalaFunctions.checkSupportedUdtf(udtf)
    val udfName = getAndValidateFunctionName(name)
    val returnColumns: Seq[UdfColumn] = udtf.outputSchema().fields.map { f =>
      UdfColumn(UdfColumnSchema(f.dataType), f.name)
    }
    val inputColumns = udtf.inputColumns
    val (code, funcBytesMap) = generateJavaUDTFCode(udtf, returnColumns, inputColumns)
    // upload dependencies and create UDF
    val needCleanupFiles = Utils.createConcurrentSet[String]()
    withUploadFailureCleanup(stageLocation, needCleanupFiles) {
      retryAfterFixingClassPath {
        val (allImports, targetJarStageLocation) =
          uploadDependencies(udfName, udtf, needCleanupFiles, funcBytesMap, stageLocation)
        // create UDTF function
        createJavaUDTF(
          returnColumns,
          inputColumns,
          udfName,
          allImports.map(i => s"'$i'").mkString(","),
          stageLocation.isEmpty,
          code,
          targetJarStageLocation)
      }
    }
    TableFunction(udfName)
  }

  // Internal function to register JavaUDTF.
  private[snowpark] def registerJavaUDTF(
      name: Option[String],
      javaUdtf: JavaUDTF,
      // if stageLocation is none, this udf will be temporary udtf
      stageLocation: Option[String] = None): TableFunction = {
    ScalaFunctions.checkSupportedJavaUdtf(javaUdtf)
    val udfName = getAndValidateFunctionName(name)
    val returnColumns = getUDFColumns(javaUdtf.outputSchema())
    val inputColumns: Seq[UdfColumn] =
      if (javaUdtfDefaultStructType.equals(javaUdtf.inputSchema())) {
        // Infer the input schema if user doesn't specify one,
        ScalaFunctions.getJavaUTFInputColumns(javaUdtf)
      } else {
        // Generate input columns from users' input
        getUDFColumns(javaUdtf.inputSchema())
      }
    val (code, funcBytesMap) = generateJavaUDTFCode(javaUdtf, returnColumns, inputColumns)
    // upload dependencies and create UDF
    val needCleanupFiles = Utils.createConcurrentSet[String]()
    withUploadFailureCleanup(stageLocation, needCleanupFiles) {
      retryAfterFixingClassPath {
        val (allImports, targetJarStageLocation) =
          uploadDependencies(udfName, javaUdtf, needCleanupFiles, funcBytesMap, stageLocation)
        // create UDTF function
        createJavaUDTF(
          returnColumns,
          inputColumns,
          udfName,
          allImports.map(i => s"'$i'").mkString(","),
          stageLocation.isEmpty,
          code,
          targetJarStageLocation)
      }
    }
    TableFunction(udfName)
  }

  private def getUDFColumns(structType: JavaStructType): Seq[UdfColumn] =
    (0 until structType.size())
      .map(structType.get)
      .map(
        field =>
          UdfColumn(
            UdfColumnSchema(JavaDataTypeUtils.javaTypeToScalaType(field.dataType)),
            field.name))

  // Clean uploaded jar files if necessary
  private def withUploadFailureCleanup[T](
      stageLocation: Option[String],
      needCleanupFiles: mutable.Set[String])(func: => Unit): Unit = {
    try {
      func
    } catch {
      case t: Throwable if stageLocation.nonEmpty =>
        try {
          needCleanupFiles.foreach { file =>
            logInfo("Removing Snowpark uploaded file: " + file)
            session.runQuery("REMOVE " + file)
            logInfo("Finished removing Snowpark uploaded file: " + file)
          }
        } catch {
          case throwable: Throwable =>
            logError("Failed to clean uploaded files", throwable)
        }
        throw t
    }
  }

  private val SCALA_MAP_STRING = "scala.collection.mutable.Map<String, String>"
  private val SCALA_MAP_VARIANT = "scala.collection.mutable.Map<String, Variant>"

  private def toOption(dataType: DataType): String = {
    s"scala.Option<${toJavaType(dataType)}>"
  }

  // The max class size is 64K, if the closure is large, UDF class compilation fails.
  // To solve this issue, the closure will be uploaded as an independent file to remote
  // and the file is included in the UDF fat jar.
  // JavaUtils.deserialize() reads the file and deserialize the closure.
  // But, it needs extra I/O to read from files, it may affect performance.
  // The final solution is:
  // If the closure is small, use inline closure; otherwise, use an independent file.
  // The experimentation indicates the max inline closure size in bytes is about
  // [9334, 9590]. Choose 8k as the threshold (~90% of max).
  private val MAX_INLINE_CLOSURE_SIZE_BYTES = 8192

  private def cleanupClosure(func: AnyRef) = {
    // Clean the closure to remove unused variables from it's capturing class.
    // Scala's compiler is not smart enough to exclude those unused variables, which caused
    // serialization issue in Jupyter Notebook and scala REPL.
    try {
      if (!session.conn.isStoredProc) {
        // In stored procedure we won't have issues that we met in Jupyter Notebook and scala REPL
        // Therefore closure cleaner is not needed.
        import com.snowflake.snowpark.internal.ClosureCleaner
        ClosureCleaner.clean(func, session.closureCleanerMode)
      }
    } catch {
      // Print a warning and continue, as closure cleaner is not always necessary.
      case e: Exception => logWarning("Closure cleaner failed with exception: " + e.getMessage)
    }
    Utils
      .getContainingClass(func)
      .foreach(clz =>
        if (classOf[scala.App].isAssignableFrom(clz)) {
          logWarning(
            "The UDF being registered may not work correctly since it is defined in a class that" +
              " extends App. Please use main() method instead of extending scala.App ")
      })
  }

  // upload dependency jars and return import_jars and target_jar on stage
  private[snowpark] def uploadDependencies(
      udfName: String,
      func: AnyRef,
      needCleanupFiles: mutable.Set[String],
      funcBytesMap: Map[String, Array[Byte]],
      // if stageLocation is none, this udf will be temporary udf
      stageLocation: Option[String]): (Seq[String], String) = {
    val actionID = session.generateNewActionID
    implicit val executionContext = session.getExecutionContext

    val qualifiedStageLocation: Option[String] = stageLocation.map(Utils.normalizeStageLocation)
    val uploadStage = qualifiedStageLocation.getOrElse(session.getSessionStage)
    val fileNameSeed = Random.nextInt().abs
    val targetJarFileName = "udfJar_" + fileNameSeed + ".jar"
    val destPrefix = Utils.getUDFUploadPrefix(udfName)
    val targetJarStageLocation = uploadStage + "/" + destPrefix + "/" + targetJarFileName
    val allFutures: ArrayBuffer[Future[String]] = ArrayBuffer[Future[String]]()
    if (!session.conn.isStoredProc) {
      // Add func's enclosing class to Classpath
      val replClassesJarContent = addClassToDependencies(func.getClass)
      val replClassesJarUploadTask = replClassesJarContent.map { bytes =>
        val jarFileName = "replClasses_" + Random.nextInt().abs + ".jar"
        val replJarStageLocation = uploadStage + "/" + jarFileName
        needCleanupFiles.add(replJarStageLocation)
        Future {
          session.conn.uploadStream(
            uploadStage,
            "",
            new ByteArrayInputStream(bytes),
            jarFileName,
            compressData = false)
          replJarStageLocation
        }
      }.toSeq
      allFutures.append(replClassesJarUploadTask: _*)
    }
    if (actionID <= session.getLastCanceledID) {
      throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
    }
    allFutures.append(session.resolveJarDependencies(uploadStage): _*)

    if (actionID <= session.getLastCanceledID) {
      throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
    }
    needCleanupFiles.add(targetJarStageLocation)

    val classDirs = UDFClassPath.classDirs(session).toList
    // Need to upload a jar if we have classDir to include or function is uploaded as a file
    if (classDirs.nonEmpty || funcBytesMap.nonEmpty) {
      val closureJarFileName = "udfJar_" + fileNameSeed + "_closure.jar"
      val closureJarStageLocation = uploadStage + "/" + destPrefix + "/" + closureJarFileName
      needCleanupFiles.add(closureJarStageLocation)
      val udfJarUploadTask = Future {
        Utils.logTime(
          createAndUploadJarToStage(
            classDirs,
            uploadStage,
            destPrefix,
            closureJarFileName,
            funcBytesMap),
          s"Uploading UDF jar to stage ${uploadStage}")
        closureJarStageLocation
      }
      allFutures.append(Seq(udfJarUploadTask): _*)
    }
    if (actionID <= session.getLastCanceledID) {
      throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
    }
    val allImports = wrapUploadTimeoutException {
      val allUrls = Await.result(
        Future.sequence(allFutures),
        FiniteDuration(session.requestTimeoutInSeconds, SECONDS))
      if (actionID <= session.getLastCanceledID) {
        throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
      }
      allUrls
    }
    (allImports, targetJarStageLocation)
  }

  // Generate Java UDTF OutputRow class for example:
  //
  //   static class OutputRow {
  //    public java.lang.String WORD;
  //    public java.lang.Integer COUNT;
  //    public OutputRow(java.lang.String WORD, java.lang.Integer COUNT) {
  //      this.WORD = WORD;
  //      this.COUNT = COUNT;
  //    }
  //  }
  private def generateUDTFOutputRow(returnColumns: Seq[UdfColumn]): String = {
    val typeNamePairs = returnColumns.map { x =>
      (toUDFArgumentType(x.schema.dataType), x.name)
    }
    val publicMembers = typeNamePairs
      .map { x =>
        s"    public ${x._1} ${x._2};"
      }
      .mkString("\n")
    val constructorArguments = typeNamePairs
      .map { x =>
        s"${x._1} ${x._2}"
      }
      .mkString(", ")
    val constructorImplementation = typeNamePairs
      .map { x =>
        s"      this.${x._2} = ${x._2};"
      }
      .mkString("\n")

    s"""
       |  static class OutputRow {
       |$publicMembers
       |    public OutputRow($constructorArguments) {
       |$constructorImplementation
       |    }
       |  }""".stripMargin
  }

  // Generate function declaration args in java function syntax e.g.
  // "Integer arg1, String arg2"
  private def generateFunctionInputArguments(inputColumns: Seq[UdfColumn]): String =
    inputColumns
      .map { arg =>
        s"${toUDFArgumentType(arg.schema.dataType)} ${arg.name}"
      }
      .mkString(",")

  // Generate get values from Row for converting it to OutputRow e.g.
  // "getInt(0), getString(1)"
  private def generateUDTFGetRowValues(row: String, returnColumns: Seq[UdfColumn]): Seq[String] =
    returnColumns
      .map(_.schema.dataType)
      .zipWithIndex
      .map { x =>
        val getValue = x._1 match {
          case BooleanType => s"$row.getBoolean(${x._2})"
          // case ByteType => s"$row.getByte(${x._2})" // UDF/UDTF doesn't support Byte.
          case ShortType => s"$row.getShort(${x._2})"
          case IntegerType => s"$row.getInt(${x._2})"
          case LongType => s"$row.getLong(${x._2})"
          case FloatType => s"$row.getFloat(${x._2})"
          case DoubleType => s"$row.getDouble(${x._2})"
          case DecimalType(_, _) => s"$row.getDecimal(${x._2})"
          case StringType => s"$row.getString(${x._2})"
          case BinaryType => s"$row.getBinary(${x._2})"
          case TimeType => s"$row.getTime(${x._2})"
          case DateType => s"$row.getDate(${x._2})"
          case TimestampType => s"$row.getTimestamp(${x._2})"
          case ArrayType(StringType) =>
            s"JavaUtils.variantToStringArray($row.getVariant(${x._2}))"
          case MapType(StringType, StringType) =>
            s"JavaUtils.variantToStringMap($row.getVariant(${x._2}))"
          // Java UDF/UDTF doesn't support Variant directly,
          // so Use String instead of Variant in OutputRow
          case VariantType =>
            s"JavaUtils.variantToString($row.getVariant(${x._2}))"
          case ArrayType(VariantType) =>
            s"JavaUtils.variantToStringArray($row.getVariant(${x._2}))"
          case MapType(StringType, VariantType) =>
            s"JavaUtils.variantToStringMap($row.getVariant(${x._2}))"
          // GeographyType is not supported yet
          // case GeographyType => s"JavaUtils.geographyToString($value)"
          case t => throw new UnsupportedOperationException(s"Unsupported type $t")
        }
        s"($row.isNullAt(${x._2}) ? null : $getValue)"
      }

  /*
   * This function generates the UDTF class signature in java.
   * For example, for "UDTF2[Option[Int], String]", this method will return
   * "com.snowflake.snowpark.udtf.UDTF2<scala.Option<java.lang.Integer>, java.lang.String>"
   */
  private[snowpark] def generateUDTFClassSignature(
      udtf: Any,
      inputColumns: Seq[UdfColumn],
      isScala: Boolean = true): String = {
    // Scala function Signature has to use scala type instead of java type
    val typeArgs = if (inputColumns.nonEmpty) {
      if (isScala) {
        inputColumns.map(arg => convertToScalaType(arg.schema)).mkString("<", ", ", ">")
      } else {
        inputColumns.map(arg => convertToJavaType(arg.schema)).mkString("<", ", ", ">")
      }
    } else {
      ""
    }
    ScalaFunctions.getUDTFClassName(udtf) + typeArgs
  }

  private def generateJavaUDTFCode(
      udtf: Any,
      returnColumns: Seq[UdfColumn],
      inputColumns: Seq[UdfColumn]): (String, Map[String, Array[Byte]]) = {
    val isScala: Boolean = udtf match {
      case _: UDTF => true
      case _: JavaUDTF => false
    }
    val outputClass = generateUDTFOutputRow(returnColumns)
    val funcBytes = JavaUtils.serialize(udtf)
    // If the closure is big, closure needs to be uploaded as an independent file to
    // remote and the file is included in the UDF fat jar.
    // For more details, refer to the comment of MAX_INLINE_CLOSURE_SIZE_BYTES
    // If a file is used, the closure byte array will be cached in as static variable
    // to avoid reading file multiple times.
    val (closureDefinition, funcBytesMap) =
      if (funcBytes.length < MAX_INLINE_CLOSURE_SIZE_BYTES) {
        (s"byte[] closure = { ${funcBytes.mkString(",")} };", Map.empty[String, Array[Byte]])
      } else {
        val fileName = "UDTFClosure" + Random.nextInt().abs + ".bin"
        (s"""byte[] closure = readClosure("$fileName");""", Map(fileName -> funcBytes))
      }

    val readClosureFileToStaticField = if (funcBytesMap.nonEmpty) {
      s"""
         |  private static byte[] udtfBytes = null;
         |
         |  private static byte[] readClosure(String fileName) {
         |    if (udtfBytes == null) {
         |      udtfBytes = JavaUtils.readFileAsByteArray(fileName);
         |    }
         |    return udtfBytes;
         |  }
         |""".stripMargin
    } else {
      ""
    }
    val udtfClassSignature = generateUDTFClassSignature(udtf, inputColumns, isScala)
    val declareArgs = generateFunctionInputArguments(inputColumns)
    val callArgs = getFunctionCallArguments(inputColumns, isScala).mkString(", ")
    val convertRowLambda = generateUDTFGetRowValues("row", returnColumns)
      .mkString("(Row row) -> new OutputRow(", ", ", ")")
    val code = if (isScala) {
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark.types.Geography;
         |import com.snowflake.snowpark.types.Variant;
         |import com.snowflake.snowpark.Row;
         |import java.util.Spliterator;
         |import java.util.Spliterators;
         |import java.util.stream.Stream;
         |import java.util.stream.StreamSupport;
         |import scala.collection.Iterator;
         |import scala.collection.JavaConverters;
         |import scala.Option;
         |
         |public class $udtfClassName {
         |  $outputClass
         |
         |  public static Class getOutputClass() {
         |    return OutputRow.class;
         |  }
         |  $readClosureFileToStaticField
         |  private $udtfClassSignature udtf;
         |
         |  public $udtfClassName() {
         |    $closureDefinition
         |    this.udtf = ($udtfClassSignature) JavaUtils.deserialize(closure);
         |  }
         |
         |  private static Stream<OutputRow> convertResult(Iterator<Row> iterator) {
         |    Spliterator<Row> spliterator =
         |      Spliterators.spliteratorUnknownSize(JavaConverters.asJavaIterator(iterator), 0);
         |    Stream<Row> rowStream = StreamSupport.stream(spliterator, false);
         |    return rowStream.map($convertRowLambda);
         |  }
         |
         |  public Stream<OutputRow> process($declareArgs) {
         |    return convertResult(udtf.process($callArgs).iterator());
         |  }
         |
         |  public Stream<OutputRow> endPartition() {
         |    return convertResult(udtf.endPartition().iterator());
         |  }
         |}""".stripMargin
    } else {
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark_java.types.Geography;
         |import com.snowflake.snowpark_java.types.Variant;
         |import com.snowflake.snowpark_java.Row;
         |import java.util.stream.Stream;
         |
         |public class $udtfClassName {
         |  $outputClass
         |
         |  public static Class getOutputClass() {
         |    return OutputRow.class;
         |  }
         |  $readClosureFileToStaticField
         |  private $udtfClassSignature udtf;
         |
         |  public $udtfClassName() {
         |    $closureDefinition
         |    this.udtf = ($udtfClassSignature) JavaUtils.deserialize(closure);
         |  }
         |
         |  public Stream<OutputRow> process($declareArgs) {
         |    Stream<Row> intermediateResult = udtf.process($callArgs);
         |    return intermediateResult.map($convertRowLambda);
         |  }
         |
         |  public Stream<OutputRow> endPartition() {
         |    Stream<Row> intermediateResult = udtf.endPartition();
         |    return intermediateResult.map($convertRowLambda);
         |  }
         |}""".stripMargin
    }
    logDebug(code)
    (code, funcBytesMap)
  }

  private[snowpark] def createJavaUDTF(
      returnDataType: Seq[UdfColumn],
      inputArgs: Seq[UdfColumn],
      udfName: String,
      allImports: String,
      isTemporary: Boolean,
      code: String,
      targetJarStageLocation: String): Unit = {
    val returnSqlType = returnDataType
      .map { x =>
        s"${x.name} ${convertToSFType(x.schema.dataType)}"
      }
      .mkString(" TABLE (", ", ", ")")
    val inputSqlTypes = inputArgs.map(arg => convertToSFType(arg.schema.dataType))
    // Create args string in SQL function syntax like "arg1 Integer, arg2 String"
    val sqlFunctionArgs =
      inputArgs.map(_.name).zip(inputSqlTypes).map { case (a, t) => s"$a $t" }.mkString(", ")

    val tempType: TempType = session.getTempType(isTemporary, udfName)
    val dropFunctionIdentifier = s"$udfName(${inputSqlTypes.mkString(",")})"
    session.recordTempObjectIfNecessary(TempObjectType.Function, dropFunctionIdentifier, tempType)

    val packageSql = if (session.packageNames.nonEmpty) {
      s"packages=(${session.packageNames.map(p => s"'$p'").toSet.mkString(", ")}) "
    } else ""
    val createUdfQuery = s"CREATE $tempType " +
      s"FUNCTION $udfName($sqlFunctionArgs) RETURNS " +
      s"$returnSqlType LANGUAGE JAVA IMPORTS = ($allImports) HANDLER='$udtfClassName' " +
      s"target_path='$targetJarStageLocation' " + packageSql +
      "AS $$ \n" + code + "\n$$"
    logInfo(s"""
               |----------SNOW----------
               |  $createUdfQuery
               |------------------------
               |""".stripMargin)
    session.runQuery(createUdfQuery)
  }

  private def generateJavaSPCode(
      func: AnyRef,
      returnValue: UdfColumnSchema,
      inputArgs: Seq[UdfColumn]): (String, Map[String, Array[Byte]]) = {
    val isScalaSP = !func.isInstanceOf[JavaSProc]
    val returnType = toUDFArgumentType(returnValue.dataType)
    val numArgs = inputArgs.length + 1
    val functionArgs = {
      val args = generateFunctionInputArguments(inputArgs)
      s"""Session session${if (args.isEmpty) "" else ", " + args}"""
    }
    val funcSignature = {
      if (isScalaSP) scalaFunctionSignature(inputArgs, returnValue)
      else javaFunctionSignature(inputArgs, returnValue)
    }
    val funcBytes = JavaUtils.serialize(func)
    val (closureDefinition, funcBytesMap) =
      if (funcBytes.length < MAX_INLINE_CLOSURE_SIZE_BYTES) {
        (s"""byte[] closure = { ${funcBytes.mkString(",")} };""", Map.empty[String, Array[Byte]])
      } else {
        val closureFileName = "Closure" + Random.nextInt().abs + ".bin"
        (s"""String closure = "$closureFileName";""", Map(closureFileName -> funcBytes))
      }
    // Apply converters to input arguments to convert from Java Type to Scala Type
    val arguments = getFunctionCallArguments(inputArgs, isScalaSP)
    val code = if (isScalaSP) {
      val callLambda =
        convertScalaReturnValue(returnValue, s"""funcImpl.apply(${("session" +: arguments)
          .mkString(",")})""")
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark.types.Geography;
         |import com.snowflake.snowpark.types.Variant;
         |import scala.collection.JavaConverters;
         |import com.snowflake.snowpark.Session;
         |
         |public class $className {
         |  private scala.Function$numArgs<Session,$funcSignature> funcImpl = null;
         |  public $className() {
         |    $closureDefinition
         |    funcImpl = (scala.Function$numArgs<Session,$funcSignature>)
         |      JavaUtils.deserialize(closure);
         |   }
         |
         |  public $returnType $methodName($functionArgs) {
         |      return $callLambda;
         |  }
         |}
         |""".stripMargin
    } else {
      val callLambda =
        convertReturnValue(returnValue, s"""funcImpl.call(${("session" +: arguments)
          .mkString(",")})""")
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark_java.types.Geography;
         |import com.snowflake.snowpark_java.types.Variant;
         |import com.snowflake.snowpark_java.Session;
         |import com.snowflake.snowpark_java.sproc.JavaSProc${numArgs - 1};
         |
         |public class $className {
         |  private JavaSProc${numArgs - 1}<$funcSignature> funcImpl = null;
         |  public $className() {
         |    $closureDefinition
         |    funcImpl = (JavaSProc${numArgs - 1}<$funcSignature>) JavaUtils.deserialize(closure);
         |   }
         |
         |  public $returnType $methodName($functionArgs) {
         |      return $callLambda;
         |  }
         |}
         |""".stripMargin
    }
    logDebug(code)
    (code, funcBytesMap)
  }

  private def generateJavaUDFCode(
      func: AnyRef,
      returnValue: UdfColumnSchema,
      inputArgs: Seq[UdfColumn]): (String, Map[String, Array[Byte]]) = {
    val isScalaUDF = !func.isInstanceOf[JavaUDF]

    val returnType = toUDFArgumentType(returnValue.dataType)

    val numArgs = inputArgs.length
    // Create args string in java function syntax like "Integer arg1, String arg2"
    val functionArgs = generateFunctionInputArguments(inputArgs)
    val funcSignature =
      if (isScalaUDF) scalaFunctionSignature(inputArgs, returnValue)
      else javaFunctionSignature(inputArgs, returnValue)
    val funcBytes = JavaUtils.serialize(func)
    // If the closure is big, closure needs to be uploaded as an independent file to
    // remote and the file is included in the UDF fat jar.
    // For more details, refer to the comment of MAX_INLINE_CLOSURE_SIZE_BYTES
    val (closureDefinition, funcBytesMap) =
      if (funcBytes.length < MAX_INLINE_CLOSURE_SIZE_BYTES) {
        (s"byte[] closure = { ${funcBytes.mkString(",")} };", Map.empty[String, Array[Byte]])
      } else {
        val closureFileName = "Closure" + Random.nextInt().abs + ".bin"
        (s"""String closure = "$closureFileName";""", Map(closureFileName -> funcBytes))
      }

    // Apply converters to input arguments to convert from Java Type to Scala Type
    val arguments = getFunctionCallArguments(inputArgs, isScalaUDF)

    val code = if (isScalaUDF) {
      val callLambda =
        convertScalaReturnValue(returnValue, s"funcImpl.apply(${arguments.mkString(",")})")
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark.types.Geography;
         |import com.snowflake.snowpark.types.Variant;
         |import scala.collection.JavaConverters;
         |
         |public class $className {
         |  private scala.Function$numArgs<$funcSignature> funcImpl = null;
         |  public $className() {
         |    $closureDefinition
         |    funcImpl = (scala.Function$numArgs<$funcSignature>) JavaUtils.deserialize(closure);
         |   }
         |
         |  public $returnType $methodName($functionArgs) {
         |      return $callLambda;
         |  }
         |}
        """.stripMargin
    } else {
      val callLambda =
        convertReturnValue(returnValue, s"funcImpl.call(${arguments.mkString(",")})")
      s"""
         |import com.snowflake.snowpark.internal.JavaUtils;
         |import com.snowflake.snowpark_java.types.Geography;
         |import com.snowflake.snowpark_java.types.Variant;
         |import com.snowflake.snowpark_java.udf.*;
         |
         |public class $className {
         |  private JavaUDF$numArgs<$funcSignature> funcImpl = null;
         |  public $className() {
         |    $closureDefinition
         |    funcImpl = (JavaUDF$numArgs<$funcSignature>) JavaUtils.deserialize(closure);
         |   }
         |  public $returnType $methodName($functionArgs) {
         |      return $callLambda;
         |  }
         |}
        """.stripMargin
    }
    logDebug(code)
    (code, funcBytesMap)
  }

  // Apply converters to input arguments to convert from Java Type to Scala Type
  private def getFunctionCallArguments(
      inputArgs: Seq[UdfColumn],
      isScalaUDF: Boolean): Seq[String] = {
    inputArgs.map(arg =>
      arg.schema.dataType match {
        case _: DataType if arg.schema.isOption => s"scala.Option.apply(${arg.name})"
        case MapType(_, StringType) if isScalaUDF => s"JavaConverters.mapAsScalaMap(${arg.name})"
        case MapType(_, VariantType) if isScalaUDF =>
          s"JavaUtils.stringMapToVariantMap(${arg.name})"
        case MapType(_, VariantType) => s"JavaUtils.stringMapToJavaVariantMap(${arg.name})"
        case ArrayType(VariantType) if isScalaUDF =>
          s"JavaUtils.stringArrayToVariantArray(${arg.name})"
        case ArrayType(VariantType) => s"JavaUtils.stringArrayToJavaVariantArray(${arg.name})"
        case GeographyType if isScalaUDF => s"JavaUtils.stringToGeography(${arg.name})"
        case GeographyType => s"JavaUtils.stringToJavaGeography(${arg.name})"
        case VariantType if isScalaUDF => s"JavaUtils.stringToVariant(${arg.name})"
        case VariantType => s"JavaUtils.stringToJavaVariant(${arg.name})"
        case _ => arg.name
    })
  }

  // Apply converter to return value to convert from Scala Type to Java Type
  private def convertScalaReturnValue(returnValue: UdfColumnSchema, value: String): String = {
    returnValue.dataType match {
      case _: DataType if returnValue.isOption => s"JavaUtils.get($value)"
      // cast returned value to scala map type and then convert to Java Map because
      // Java UDFs only support Java Map as return type.
      case MapType(_, StringType) => s"JavaConverters.mapAsJavaMap($value)"
      case MapType(_, VariantType) => s"JavaUtils.variantMapToStringMap($value)"
      case _ => convertReturnValue(returnValue, value)
    }
  }

  private def convertReturnValue(returnValue: UdfColumnSchema, value: String): String = {
    returnValue.dataType match {
      case GeographyType => s"JavaUtils.geographyToString($value)"
      case VariantType => s"JavaUtils.variantToString($value)"
      case MapType(_, VariantType) => s"JavaUtils.javaVariantMapToStringMap($value)"
      case ArrayType(VariantType) => s"JavaUtils.variantArrayToStringArray($value)"
      case _ => s"$value"
    }
  }

  private def convertToScalaType(columnSchema: UdfColumnSchema): String = {
    columnSchema.dataType match {
      case t: DataType if columnSchema.isOption => toOption(t)
      case MapType(_, VariantType) => SCALA_MAP_VARIANT
      case MapType(_, StringType) => SCALA_MAP_STRING
      case ArrayType(VariantType) => "Variant[]"
      case _ => toJavaType(columnSchema.dataType)
    }
  }

  private def convertToJavaType(columnSchema: UdfColumnSchema): String = {
    columnSchema.dataType match {
      case t: DataType if columnSchema.isOption => toOption(t)
      case MapType(_, VariantType) => "java.util.Map<String,Variant>"
      case ArrayType(VariantType) => "Variant[]"
      case _ => toJavaType(columnSchema.dataType)
    }
  }

  /*
   * This function maps the types to the corresponding
   * scala types and generates a comma-separated
   * string with types. For example, for a scala function that
   * take two Option[Int] and returns a String, this method will return
   * "scala.Option<java.lang.Integer>,scala.Option<java.lang.Integer>, java.lang.String"
   *
   */
  private def scalaFunctionSignature(
      inputArgs: Seq[UdfColumn],
      returnValue: UdfColumnSchema): String = {
    // Scala function Signature has to use scala type instead of java type
    val inputScalaTypes = inputArgs.map(arg => convertToScalaType(arg.schema))
    val returnTypeInFunc = convertToScalaType(returnValue)
    inputScalaTypes.mkString(",") + (if (inputScalaTypes.isEmpty) "" else ",") + returnTypeInFunc
  }

  private def javaFunctionSignature(
      inputArgs: Seq[UdfColumn],
      returnValue: UdfColumnSchema): String = {
    // Scala function Signature has to use scala type instead of java type
    val inputScalaTypes = inputArgs.map(arg => convertToJavaType(arg.schema))
    val returnTypeInFunc = convertToJavaType(returnValue)
    inputScalaTypes.mkString(",") + (if (inputScalaTypes.isEmpty) "" else ",") + returnTypeInFunc
  }

  private def createJavaSP(
      returnDataType: DataType,
      inputArgs: Seq[UdfColumn],
      spName: String,
      allImports: String,
      isTemporary: Boolean,
      code: String,
      targetJarStageLocation: String,
      isCallerMode: Boolean): Unit = {
    val returnSqlType = convertToSFType(returnDataType)
    val inputSqlTypes = inputArgs.map(arg => convertToSFType(arg.schema.dataType))
    val sqlFunctionArgs = inputArgs
      .map(_.name)
      .zip(inputSqlTypes)
      .map {
        case (a, t) => s"$a $t"
      }
      .mkString(",")
    val tempType: TempType = if (isTemporary) TempType.Temporary else TempType.Permanent
    val dropFunctionIdentifier = s"$spName(${inputSqlTypes.mkString(",")})"
    session.recordTempObjectIfNecessary(
      TempObjectType.Procedure,
      dropFunctionIdentifier,
      tempType)
    val packageSql = if (session.packageNames.nonEmpty) {
      s"packages=(${session.packageNames.map(p => s"'$p'").toSet.mkString(",")})"
    } else ""
    val importSql = if (allImports.nonEmpty) {
      s"IMPORTS = ($allImports)"
    } else ""
    val mode = if (isCallerMode) "EXECUTE AS CALLER" else ""
    val createSPQuery =
      s"""
         |CREATE $tempType PROCEDURE $spName($sqlFunctionArgs)
         |RETURNS $returnSqlType
         |LANGUAGE JAVA
         |RUNTIME_VERSION = '11'
         |$packageSql
         |$importSql
         |target_path='$targetJarStageLocation'
         |HANDLER='$className.$methodName'
         |$mode
         |AS
         |$$$$
         |$code
         |$$$$""".stripMargin
    logDebug(s"""
                |----------SNOW----------
                |  $createSPQuery
                |------------------------
                |""".stripMargin)
    session.runQuery(createSPQuery, isTemporary)
  }

  // no test coverage of this function since temporary removed all usage.
  // will re-use function in Java UDF API
  private[snowpark] def createJavaUDF(
      returnDataType: DataType,
      inputArgs: Seq[UdfColumn],
      udfName: String,
      allImports: String,
      isTemporary: Boolean,
      code: String,
      targetJarStageLocation: String): Unit = {
    val returnSqlType = convertToSFType(returnDataType)
    val inputSqlTypes = inputArgs.map(arg => convertToSFType(arg.schema.dataType))
    // Create args string in SQL function syntax like "arg1 Integer, arg2 String"
    val sqlFunctionArgs =
      inputArgs.map(_.name).zip(inputSqlTypes).map { case (a, t) => s"$a $t" }.mkString(",")

    val tempType: TempType = session.getTempType(isTemporary, udfName.split("\\.").last)
    val dropFunctionIdentifier = s"$udfName(${inputSqlTypes.mkString(",")})"
    session.recordTempObjectIfNecessary(TempObjectType.Function, dropFunctionIdentifier, tempType)

    val packageSql = if (session.packageNames.nonEmpty) {
      s"packages=(${session.packageNames.map(p => s"'$p'").toSet.mkString(", ")}) "
    } else ""
    val importSql = if (allImports.nonEmpty) {
      s"IMPORTS = ($allImports)"
    } else ""
    val createUdfQuery = s"CREATE $tempType " +
      s"FUNCTION $udfName($sqlFunctionArgs) RETURNS " +
      s"$returnSqlType LANGUAGE JAVA $getRuntimeVersion" +
      s"$importSql HANDLER='$className.$methodName' " +
      s"target_path='$targetJarStageLocation' " + packageSql +
      "AS $$ \n" + code + "\n$$"
    logDebug(s"""
               |----------SNOW----------
               |  $createUdfQuery
               |------------------------
               |""".stripMargin)
    session.runQuery(createUdfQuery, isTemporary)
  }

  private[snowpark] def addClassToDependencies(funcClass: Class[_]): Option[Array[Byte]] = {
    val localFileDependencies = if (session.packageNames.nonEmpty) {
      val requiredDependencies = UDFClassPath.classpath
        .filter(_.location.nonEmpty)
        .map(l => new File(l.location.get).toURI)
      session.getLocalFileDependencies ++ requiredDependencies.toSet
    } else {
      session.getLocalFileDependencies
    }
    if (!UDFClassPath.isClassDefined(localFileDependencies.toSeq, funcClass.getName)) {
      val classLocation = UDFClassPath.getPathUsingCodeSource(funcClass)
      val classLoader = funcClass.getClassLoader
      if (classLoader.isInstanceOf[AbstractFileClassLoader]) {
        val rootDirectory = classLoader.asInstanceOf[AbstractFileClassLoader].root
        if (rootDirectory.isInstanceOf[VirtualDirectory]) {
          logInfo(
            s"Found REPL classes in memory, uploading to stage. " +
              "Use -Yrepl-outdir <dir-name> to generate REPL classes on disk")
          Option(replClassesToJarBytes(rootDirectory))
        } else {
          logInfo(s"Automatically adding REPL directory ${rootDirectory.path} to dependencies")
          session.addDependency(rootDirectory.path)
          None
        }
      } else if (classLocation.isDefined) {
        session.addDependency(classLocation.get)
        logInfo(s"Adding ${classLocation.get} to session dependencies")
        None
      } else {
        throw ErrorMessage.UDF_CANNOT_DETECT_UDF_FUNCION_CLASS()
      }
    } else {
      None
    }
  }

  private def replClassesToJarBytes(rootDirectory: AbstractFile): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val jarOutputSteam = new java.util.jar.JarOutputStream(byteArrayOutputStream)

    // This function gets the relative path from file's abstract path.
    // AbstractFile.path() returns the path which includes the root directory name.
    // when the class file is achieved into Jar file, it uses relative path.
    def getRelativePath(path: String): String =
      path.substring(rootDirectory.name.length + 1)

    def writeToJar(file: AbstractFile): Unit = {
      if (file.isDirectory) {
        file.iterator.foreach(writeToJar)
      } else {
        jarOutputSteam.putNextEntry(new java.util.jar.JarEntry(getRelativePath(file.path)))
        jarOutputSteam.write(file.toByteArray)
        jarOutputSteam.closeEntry
      }
    }

    writeToJar(rootDirectory)
    jarOutputSteam.close()
    byteArrayOutputStream.toByteArray
  }

  /**
   * This method uses the Piped{Input/Output}Stream classes to create an
   * in-memory jar file and write to a snowflake stage in parallel in two threads.
   * This design is not the most-efficient since the implementation of
   * PipedInputStream puts the thread to sleep for 1 sec if it is waiting to read/write data.
   * But this is still faster than writing stream to a temp file.
   *
   * @param classDirs class directories that are copied to the jar
   * @param stageName Name of stage
   * @param destPrefix Destination prefix
   * @param jarFileName Name of the jar file
   * @param funcBytesMap func bytes map (entry format: fileName -> funcBytes)
   * @since 0.1.0
   */
  private[snowpark] def createAndUploadJarToStage(
      classDirs: List[File],
      stageName: String,
      destPrefix: String,
      jarFileName: String,
      funcBytesMap: Map[String, Array[Byte]]): Unit =
    Utils.withRetry(
      session.maxFileUploadRetryCount,
      s"Uploading UDF jar: $destPrefix $jarFileName $stageName $classDirs") {
      createAndUploadJarToStageInternal(
        classDirs,
        stageName,
        destPrefix,
        jarFileName,
        funcBytesMap)
    }

  private def createAndUploadJarToStageInternal(
      classDirs: List[File],
      stageName: String,
      destPrefix: String,
      jarFileName: String,
      funcBytesMap: Map[String, Array[Byte]]): Unit = {
    classDirs.foreach(dir => logInfo(s"Adding directory ${dir.toString} to UDF jar"))

    val source = new PipedOutputStream()
    val sink = new PipedInputStream(source, 4096)
    val jarStream = new JarOutputStream(source)

    var readError: Option[Throwable] = None
    val pipeWriter = new Thread {
      override def run {
        try {
          jarBuilder.createFatJar(List.empty, classDirs, List.empty, funcBytesMap, jarStream)
        } catch {
          case t: Throwable =>
            logError(
              s"Error in child thread while creating udf jar: " +
                s"$classDirs $destPrefix $jarFileName $stageName")
            readError = Some(t)
            throw t
        } finally {
          jarStream.close()
        }
      }
    }

    var uploadError: Option[Throwable] = None
    val pipeReader = new Thread {
      override def run {
        try {
          session.conn.uploadStream(stageName, destPrefix, sink, jarFileName, false)
        } catch {
          case t: Throwable =>
            logError(
              s"Error in child thread while uploading udf jar: " +
                s"$classDirs $destPrefix $jarFileName $stageName")
            uploadError = Some(t)
            throw t
        } finally {
          sink.close()
        }
      }
    }
    try {
      // Start the threads
      pipeWriter.start()
      pipeReader.start()
      // Wait for threads to complete
      pipeReader.join()
      pipeWriter.join()
    } finally {
      sink.close()
    }

    if (readError.nonEmpty || uploadError.nonEmpty) {
      logError(
        s"Main udf registration thread caught an error: " +
          s"${if (uploadError.nonEmpty) s"upload error: ${uploadError.get.getMessage}" else ""}" +
          s"${if (readError.nonEmpty) s" read error: ${readError.get.getMessage}" else ""}")
      if (uploadError.nonEmpty) {
        throw uploadError.get
      } else {
        throw readError.get
      }
    }
  }

  private def getRuntimeVersion: String = {
    val version = Utils.JavaVersion
    if (version.startsWith("17")) {
      "runtime_version = '17'"
    } else {
      // for any other version of JVM, let's use the default jvm, which is java 11.
      // it is current behavior.
      ""
    }
  }
}
