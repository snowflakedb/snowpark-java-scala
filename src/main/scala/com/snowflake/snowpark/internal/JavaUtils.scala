package com.snowflake.snowpark.internal

import com.fasterxml.jackson.databind.JsonNode
import com.snowflake.snowpark.Session.SessionBuilder
import com.snowflake.snowpark.{
  Column,
  DataFrame,
  DataFrameNaFunctions,
  DataFrameStatFunctions,
  GroupingSets,
  MatchedClauseBuilder,
  MergeBuilder,
  NotMatchedClauseBuilder,
  SProcRegistration,
  Session,
  StoredProcedure,
  TableFunction,
  TypedAsyncJob,
  UDFRegistration,
  UDTFRegistration,
  Updatable,
  UpdatableAsyncActor,
  UpdateResult,
  UserDefinedFunction
}

import java.io._
import com.snowflake.snowpark.types.{Geography, Geometry, Variant}
import com.snowflake.snowpark_java.types.InternalUtils
import com.snowflake.snowpark_java.udtf._

import scala.collection.{JavaConverters, mutable}
import scala.collection.JavaConverters._

object JavaUtils {
  def session_setJavaAPI(builder: SessionBuilder): SessionBuilder =
    builder.setJavaAPI()

  def session_requestTimeoutInSeconds(session: Session): Int =
    session.requestTimeoutInSeconds

  def notMatchedClauseBuilder_insert(
      assignments: java.util.Map[Column, Column],
      builder: NotMatchedClauseBuilder): MergeBuilder =
    builder.insert(assignments.asScala.toMap)

  def notMatchedClauseBuilder_insertRow(
      assignments: java.util.Map[String, Column],
      builder: NotMatchedClauseBuilder): MergeBuilder =
    builder.insert(assignments.asScala.toMap)

  def matchedClauseBuilder_update(
      assignments: java.util.Map[Column, Column],
      builder: MatchedClauseBuilder): MergeBuilder =
    builder.update(assignments.asScala.toMap)

  def matchedClauseBuilder_updateColumn(
      assignments: java.util.Map[String, Column],
      builder: MatchedClauseBuilder): MergeBuilder =
    builder.update(assignments.asScala.toMap)

  def updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      condition: Column,
      sourceData: DataFrame,
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap, condition, sourceData)

  def async_updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      condition: Column,
      sourceData: DataFrame,
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap, condition, sourceData)

  def updatable_update(
      assignments: java.util.Map[Column, Column],
      condition: Column,
      sourceData: DataFrame,
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap, condition, sourceData)

  def async_updatable_update(
      assignments: java.util.Map[Column, Column],
      condition: Column,
      sourceData: DataFrame,
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap, condition, sourceData)

  def updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      condition: Column,
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap, condition)

  def async_updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      condition: Column,
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap, condition)

  def updatable_update(
      assignments: java.util.Map[Column, Column],
      condition: Column,
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap, condition)

  def async_updatable_update(
      assignments: java.util.Map[Column, Column],
      condition: Column,
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap, condition)

  def updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap)

  def async_updatable_updateColumn(
      assignments: java.util.Map[String, Column],
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap)

  def updatable_update(
      assignments: java.util.Map[Column, Column],
      updatable: Updatable): UpdateResult =
    updatable.update(assignments.asScala.toMap)

  def async_updatable_update(
      assignments: java.util.Map[Column, Column],
      updatable: UpdatableAsyncActor): TypedAsyncJob[UpdateResult] =
    updatable.update(assignments.asScala.toMap)

  def replacement(
      colName: String,
      replacement: java.util.Map[_, _],
      func: DataFrameNaFunctions): DataFrame =
    func.replace(colName, replacement.asScala.toMap)

  def fill(map: java.util.Map[String, _], func: DataFrameNaFunctions): DataFrame =
    func.fill(map.asScala.toMap)

  def sampleBy(
      col: Column,
      fractions: java.util.Map[_, _],
      func: DataFrameStatFunctions): DataFrame = {
    val scalaMap = fractions.asScala.map { case (key, value) =>
      key -> value.asInstanceOf[Double]
    }.toMap
    func.sampleBy(col, scalaMap)
  }

  def sampleBy(
      col: String,
      fractions: java.util.Map[_, _],
      func: DataFrameStatFunctions): DataFrame = {
    val scalaMap = fractions.asScala.map { case (key, value) =>
      key -> value.asInstanceOf[Double]
    }.toMap
    func.sampleBy(col, scalaMap)
  }

  def javaSaveModeToScala(
      mode: com.snowflake.snowpark_java.SaveMode): com.snowflake.snowpark.SaveMode = {
    mode match {
      case com.snowflake.snowpark_java.SaveMode.Append => com.snowflake.snowpark.SaveMode.Append
      case com.snowflake.snowpark_java.SaveMode.Ignore => com.snowflake.snowpark.SaveMode.Ignore
      case com.snowflake.snowpark_java.SaveMode.Overwrite =>
        com.snowflake.snowpark.SaveMode.Overwrite
      case com.snowflake.snowpark_java.SaveMode.ErrorIfExists =>
        com.snowflake.snowpark.SaveMode.ErrorIfExists
    }
  }

  def objectArrayToSeq(arr: Array[_]): Seq[Any] = arr

  def seqToList[T](seq: Seq[T]): java.util.List[T] = seq.asJava

  def get[T](o: Option[T]): T = o.getOrElse(null.asInstanceOf[T])

  def geographyToString(g: Geography): String = if (g == null) null else g.asGeoJSON()

  def geometryToString(g: Geometry): String = if (g == null) null else g.toString()

  def geographyToString(g: com.snowflake.snowpark_java.types.Geography): String =
    if (g == null) null else g.asGeoJSON()

  def geometryToString(g: com.snowflake.snowpark_java.types.Geometry): String =
    if (g == null) null else g.toString

  def stringToGeography(g: String): Geography = if (g == null) null else Geography.fromGeoJSON(g)

  def stringToGeometry(g: String): Geometry = if (g == null) null else Geometry.fromGeoJSON(g)

  def stringToJavaGeography(g: String): com.snowflake.snowpark_java.types.Geography =
    if (g == null) null else com.snowflake.snowpark_java.types.Geography.fromGeoJSON(g)

  def stringToJavaGeometry(g: String): com.snowflake.snowpark_java.types.Geometry =
    if (g == null) null else com.snowflake.snowpark_java.types.Geometry.fromGeoJSON(g)

  def variantToString(v: Variant): String = if (v == null) null else v.asJsonString()

  def variantToString(v: com.snowflake.snowpark_java.types.Variant): String =
    if (v == null) null else v.asJsonString()

  def variantToStringArray(v: Variant): Array[String] =
    if (v == null) null else v.asArray().map(_.toString)

  def variantToStringArray(v: com.snowflake.snowpark_java.types.Variant): Array[String] =
    if (v == null) null else v.asArray().map(_.toString)

  def variantToStringMap(v: Variant): java.util.Map[String, String] =
    if (v == null) null else v.asMap().map(e => (e._1, e._2.toString)).asJava

  def variantToStringMap(
      v: com.snowflake.snowpark_java.types.Variant): java.util.Map[String, String] =
    if (v == null) null
    else {
      InternalUtils
        .toScalaVariant(v)
        .asMap()
        .map(e => (e._1, e._2.toString))
        .asJava
    }

  def stringToVariant(v: String): Variant = if (v == null) null else new Variant(v)

  def stringToJavaVariant(v: String): com.snowflake.snowpark_java.types.Variant =
    if (v == null) null else new com.snowflake.snowpark_java.types.Variant(v)

  def variantArrayToStringArray(v: Array[Variant]): Array[String] =
    if (v == null) null else v.map(e => variantToString(e))

  def variantArrayToStringArray(
      v: Array[com.snowflake.snowpark_java.types.Variant]): Array[String] =
    if (v == null) null else v.map(e => variantToString(e))

  def stringArrayToVariantArray(v: Array[String]): Array[Variant] =
    if (v == null) null else v.map(e => stringToVariant(e))

  def stringArrayToJavaVariantArray(
      v: Array[String]): Array[com.snowflake.snowpark_java.types.Variant] =
    if (v == null) null else v.map(e => stringToJavaVariant(e))

  def variantMapToStringMap(v: mutable.Map[String, Variant]): java.util.Map[String, String] =
    if (v == null) null
    else JavaConverters.mapAsJavaMap(v.map(e => (e._1, variantToString(e._2))))

  def variantMapToStringMap(v: java.util.Map[String, Variant]): java.util.Map[String, String] =
    if (v == null) null
    else {
      val result = new java.util.HashMap[String, String]()
      v.entrySet().forEach(entry => result.put(entry.getKey, variantToString(entry.getValue)))
      result
    }

  def javaVariantMapToStringMap(v: java.util.Map[String, com.snowflake.snowpark_java.types.Variant])
      : java.util.Map[String, String] =
    if (v == null) null
    else {
      val result = new java.util.HashMap[String, String]()
      v.entrySet().forEach(entry => result.put(entry.getKey, variantToString(entry.getValue)))
      result
    }

  def createScalaVariant(value: JsonNode, dataType: String): Variant =
    new Variant(value, Variant.VariantTypes.getType(dataType))

  def getVariantValue(variant: Variant): JsonNode = variant.value

  def getVariantType(variant: Variant): String = variant.dataType.toString

  def stringMapToVariantMap(v: java.util.Map[String, String]): mutable.Map[String, Variant] =
    if (v == null) null
    else JavaConverters.mapAsScalaMap(v).map(e => (e._1, stringToVariant(e._2)))

  def stringMapToVariantJavaMap(v: java.util.Map[String, String]): java.util.Map[String, Variant] =
    if (v == null) null
    else {
      val result = new java.util.HashMap[String, Variant]()
      v.entrySet().forEach(entry => result.put(entry.getKey, stringToVariant(entry.getValue)))
      result
    }

  def stringMapToJavaVariantMap(v: java.util.Map[String, String])
      : java.util.Map[String, com.snowflake.snowpark_java.types.Variant] =
    if (v == null) null
    else {
      val result = new java.util.HashMap[String, com.snowflake.snowpark_java.types.Variant]()
      v.entrySet().forEach(entry => result.put(entry.getKey, stringToJavaVariant(entry.getValue)))
      result
    }

  def javaStringColumnMapToScala(input: java.util.Map[String, Column]): Map[String, Column] =
    input.asScala.toMap

  def javaStringAnyMapToScala(map: java.util.Map[String, _]): Map[String, Any] =
    map.asScala.toMap

  def setToJavaSet[T](set: Set[T]): java.util.Set[T] = {
    set.asJava
  }

  def createGroupingSets(sets: Array[java.util.Set[Column]]): GroupingSets = {
    GroupingSets(sets.map(set => set.asScala.toSet).toSeq)
  }

  def groupingSetArrayToSeq(sets: Array[GroupingSets]): Seq[GroupingSets] = sets

  def seqToJavaStringArray(seq: Seq[String]): Array[String] =
    seq.toArray

  def columnArrayToSeq(arr: Array[Column]): Seq[Column] = arr

  def columnArrayToAnySeq(arr: Array[Column]): Seq[Any] = arr

  def stringArrayToStringSeq(arr: Array[String]): Seq[String] = arr

  def objectListToAnySeq(input: java.util.List[java.util.List[Object]]): Seq[Seq[Any]] =
    input.asScala.map(list => list.asScala)

  def registerUDF(
      udfRegistration: UDFRegistration,
      name: String,
      udf: UserDefinedFunction,
      stageLocation: String): UserDefinedFunction =
    udfRegistration.register(Option(name), udf, Option(stageLocation))

  def registerJavaUDTF(
      udtfRegistration: UDTFRegistration,
      name: String,
      javaUdtf: JavaUDTF,
      stageLocation: String): TableFunction =
    udtfRegistration.registerJavaUDTF(Option(name), javaUdtf, Option(stageLocation))

  def registerJavaSProc(
      sprocRegistration: SProcRegistration,
      name: String,
      sp: StoredProcedure,
      stageLocation: String): StoredProcedure =
    sprocRegistration.register(Option(name), sp, Option(stageLocation))

  def registerJavaSProc(
      sprocRegistration: SProcRegistration,
      name: String,
      sp: StoredProcedure,
      stageLocation: String,
      isCallerMode: Boolean): StoredProcedure =
    sprocRegistration.register(Option(name), sp, Option(stageLocation), isCallerMode)

  def getActiveSession: Session =
    Session.getActiveSession.getOrElse(throw ErrorMessage.UDF_NO_DEFAULT_SESSION_FOUND())

  // `char` is a Java key word, so Java API can't directly access functions.char
  def charFunc(col: Column): Column = com.snowflake.snowpark.functions.char(col)

  def javaStringStringMapToScala(map: java.util.Map[String, String]): Map[String, String] =
    map.asScala.toMap

  def javaMapToScalaWithVariantConversion(map: java.util.Map[_, _]): Map[Any, Any] =
    map.asScala.map {
      case (key, value: com.snowflake.snowpark_java.types.Variant) =>
        key -> InternalUtils.toScalaVariant(value)
      case (key, value) => key -> value
    }.toMap

  def scalaMapToJavaWithVariantConversion(map: Map[_, _]): java.util.Map[Object, Object] =
    map.map {
      case (key, value: com.snowflake.snowpark.types.Variant) =>
        key.asInstanceOf[Object] -> InternalUtils.createVariant(value)
      case (key, value) => key.asInstanceOf[Object] -> value.asInstanceOf[Object]
    }.asJava

  def serialize(obj: Any): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    var out: ObjectOutputStream = null
    try {
      out = new ObjectOutputStream(bos)
      out.writeObject(obj)
      out.flush()
      bos.toByteArray
    } finally {
      bos.close();
    }
  }

  def readFileAsByteArray(fileName: String): Array[Byte] = {
    // Need to add "/" to file name for resource search.
    val inputStream = getClass.getResourceAsStream("/" + fileName)
    if (inputStream == null) {
      throw new Exception("JavaUtils.readFileAsByteArray() cannot find file: " + fileName)
    }
    Stream.continually(inputStream.read()).takeWhile(_ != -1).map(_.toByte).toArray
  }

  def deserialize(bytes: Array[Byte]): Object = {
    val bis = new ByteArrayInputStream(bytes)
    doDeserializeAndCloseInputStream(bis)
  }

  def deserialize(fileName: String): Object = {
    // Need to add "/" to file name for resource search.
    val fis = getClass.getResourceAsStream("/" + fileName)
    doDeserializeAndCloseInputStream(fis)
  }

  private def doDeserializeAndCloseInputStream(inputStream: InputStream): Object = {
    var in: ObjectInputStream = null
    try {
      in = new ObjectInputStream(inputStream)
      in.readObject()
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally {
      try {
        inputStream.close()
        in.close();
      } catch {
        case e: IOException => // Ignore error in closing stream
      }
    }
  }

}
