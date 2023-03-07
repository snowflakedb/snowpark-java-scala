package com.snowflake.code_verification

import com.snowflake.snowpark.internal.Logging

import java.lang.reflect.Modifier
import scala.collection.mutable
import scala.reflect.runtime.universe.{MethodSymbol, Type, TypeTag, typeOf}

object ClassUtils extends Logging {

  // return a name list of public functions
  def getAllPublicFunctionNames[T: TypeTag](clazz: Class[T]): Seq[String] = {
    val tpe = typeOf[T]

    val publicMethod: Set[String] = tpe.members.collect {
      case m if m.isPublic => m.name.toString.trim
    }.toSet

    val javaStatic = clazz.getMethods
      .filter(m => Modifier.isStatic(m.getModifiers))
      .map(_.getName)
      .toSet

    val superClassMethods = clazz.getSuperclass.getMethods.map(_.getName).toSet
    // the result doesn't include the methods from the supper class
    (publicMethod ++ javaStatic -- superClassMethods)
      .filter(m => !m.contains("$") && !m.contains("<"))
      .toSeq
  }

  /**
   * Check if two classes have same function names.
   * It is not perfect since it can only check function
   * names but not args.
   */
  def containsSameFunctionNames[A: TypeTag, B: TypeTag](
      class1: Class[A],
      class2: Class[B],
      class1Only: Set[String] = Set.empty,
      class2Only: Set[String] = Set.empty,
      class1To2NameMap: Map[String, String] = Map.empty): Boolean = {
    val nameList1 = getAllPublicFunctionNames(class1)
    val nameList2 = mutable.Set[String](getAllPublicFunctionNames(class2): _*)
    var missed = false
    // print info
    logInfo(s"""Comparing ${class1.getName} and ${class2.getName}
         |- ${class1.getName} has ${nameList1.size} functions
         |- ${class2.getName} has ${nameList2.size} functions
         |""".stripMargin)
    nameList1.foreach(name => {
      if (!class1Only.contains(name)) {
        val newName = class1To2NameMap.getOrElse(name, name)
        if (nameList2.contains(newName)) {
          nameList2.remove(newName)
        } else {
          missed = true
          logError(s"${class2.getName} misses function $newName")
        }
      }
    })
    val list2Cache = nameList2.clone()
    nameList2.foreach(name =>
      if (class2Only.contains(name)) {
        list2Cache.remove(name)
      } else {
        logError(s"${class1.getName} misses function $name")
    })
    !missed && list2Cache.isEmpty
  }
}
