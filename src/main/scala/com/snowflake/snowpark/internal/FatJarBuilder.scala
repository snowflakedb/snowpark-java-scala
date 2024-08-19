package com.snowflake.snowpark.internal

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.jar._
import org.apache.commons.io.IOUtils
import scala.collection.mutable

class FatJarBuilder {

  /** @param classFiles
    *   class bytes that are copied to the fat jar
    * @param classDirs
    *   directories from which files are copied to the fat jar
    * @param jars
    *   Jars to be copied to the fat jar
    * @param funcBytesMap
    *   func bytes map (entry format: fileName -> funcBytes)
    * @param target
    *   The outputstream the jar contents should be written to
    */
  def createFatJar(
      classFiles: List[InMemoryClassObject],
      classDirs: List[File],
      jars: List[JarFile],
      funcBytesMap: Map[String, Array[Byte]],
      target: JarOutputStream
  ): Unit = {
    val manifest = new Manifest
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")

    // Used to track already added directories
    val trackPaths = new mutable.HashSet[String]

    classFiles.foreach(copyFileToTargetJar(_, target, trackPaths))
    classDirs.foreach(copyDirToTargetJar(_, target, trackPaths))
    jars.foreach(copyJarToTargetJar(_, target, trackPaths))

    // Add function class to the Jar
    funcBytesMap.foreach { fileEntry =>
      target.putNextEntry(new java.util.jar.JarEntry(fileEntry._1))
      target.write(fileEntry._2)
      target.closeEntry
    }
  }

  /** This method adds a class file to target jar.
    * @param classObj
    *   Class file that is copied to target jar
    * @param target
    *   OutputStream for target jar
    * @param trackPaths
    *   This tracks all the directories already added to the jar
    */
  private def copyFileToTargetJar(
      classObj: InMemoryClassObject,
      target: JarOutputStream,
      trackPaths: mutable.HashSet[String]
  ): Unit = {
    val dirs = classObj.getClassName.split("\\.")
    var prefix = ""
    dirs
      .slice(0, dirs.length - 1)
      .foreach(dir => {
        val dirName = prefix + dir + "/"
        addDirEntryToJar(dirName, trackPaths, target)
        prefix = dirName
      })

    val entryName = prefix + dirs.last + ".class"
    target.putNextEntry(new JarEntry(entryName))
    target.write(classObj.getBytes())
    target.closeEntry()
  }

  /** This method recursively adds all directories and files in root dir to the target jar
    * @param root
    *   Root directory, all directories are added to the jar relative to root's path
    * @param target
    *   OutputStream for target jar
    * @param trackPaths
    *   This tracks all the directories already added to the jar
    */
  private def copyDirToTargetJar(
      root: File,
      target: JarOutputStream,
      trackPaths: mutable.HashSet[String]
  ): Unit = {
    Files.walkFileTree(
      root.toPath,
      new SimpleFileVisitor[Path]() {
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          // Only add relative path to jar
          val entryName = root.toPath.relativize(dir).toFile.getPath
          addDirEntryToJar(entryName, trackPaths, target)
          FileVisitResult.CONTINUE
        }
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          // Only add relative path to jar
          // In jar file generation, it always uses linux path format
          val entryName =
            Utils.convertPathIfNecessary(root.toPath.relativize(file).toFile.getPath)
          if (!entryName.contains("META-INF")) {
            addFileEntryToJar(entryName, Files.newInputStream(file), target)
          }
          FileVisitResult.CONTINUE
        }
      }
    )
  }

  /** This method adds all entries in source jar to the target jar
    * @param sourceJar
    *   Source directory
    * @param target
    *   OutputStream for target jar
    * @param trackPaths
    *   This tracks all the directories already added to the jar
    */
  private def copyJarToTargetJar(
      sourceJar: JarFile,
      target: JarOutputStream,
      trackPaths: mutable.HashSet[String]
  ): Unit = {
    val entries = sourceJar.entries()
    while (entries.hasMoreElements) {
      val entry = entries.nextElement()
      val entryName = entry.getName
      // Ignore files already added and Manifest files
      if (!trackPaths.contains(entryName) && !entryName.contains("META-INF")) {
        trackPaths += entryName
        addFileEntryToJar(entryName, sourceJar.getInputStream(entry), target)
      }
    }
  }

  /** This method adds a file entry into the target jar
    * @param entryName
    *   Name of entry
    * @param is
    *   Input stream to fetch file bytes, it closes the input stream once done
    * @param target
    *   OutputStream for target jar
    */
  private def addFileEntryToJar(
      entryName: String,
      is: InputStream,
      target: JarOutputStream
  ): Unit = {
    try {
      target.putNextEntry(new JarEntry(entryName))
      IOUtils.copy(is, target)
      target.closeEntry()
    } finally {
      is.close()
    }
  }

  private def addDirEntryToJar(
      entryName: String,
      trackPaths: mutable.HashSet[String],
      target: JarOutputStream
  ): Unit = {
    val dirName = if (!entryName.endsWith("/")) entryName + "/" else entryName
    if (!trackPaths.contains(dirName)) {
      trackPaths += dirName
      target.putNextEntry(new JarEntry(dirName))
      target.closeEntry()
    }
  }
}
