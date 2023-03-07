package com.snowflake.snowpark

import java.io.File
import java.util.jar.JarFile

import scala.collection.mutable

trait FileUtils {
  def listClassesInJar(fileName: String): Set[String] = {
    val classes = new mutable.HashSet[String]
    val jarFile = new JarFile(new File(fileName))
    val entries = jarFile.entries()
    while (entries.hasMoreElements) {
      val entry = entries.nextElement()
      if (entry.getName.endsWith(".class")) {
        classes.add(entry.getName)
      }
    }
    classes.toSet
  }
}
