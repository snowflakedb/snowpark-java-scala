/*
Modifications copyright (C) 2021 Snowflake Inc.
 */

package com.snowflake.snowpark.internal

import java.io._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.invoke.SerializedLambda

import com.snowflake.snowpark.SnowparkClientException
import com.snowflake.snowpark.internal.ParameterUtils.ClosureCleanerMode

import scala.tools.asm.{ClassReader, ClassVisitor, Handle, MethodVisitor, Type}
import scala.tools.asm.Opcodes._
import scala.tools.asm.tree.{ClassNode, MethodNode}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{Map, Set, Stack}

private[snowpark] object ClosureCleaner extends Logging {

  // Get an ASM class reader for a given class from the JAR that loaded it
  def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream(128)
      copyStream(resourceStream, baos, true)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  /** Copy all data from an InputStream to an OutputStream */
  def copyStream(in: InputStream, out: OutputStream, closeStreams: Boolean = false): Long = {
    var count = 0L
    try {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel
        val size = inChannel.size()

        // In case transferTo method transferred less data than we have required.
        while (count < size) {
          count += inChannel.transferTo(count, size - count, outChannel)
        }
      } else {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
      }
      count
    } finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  /** Try to get a serialized Lambda from the closure.
    *
    * @param closure
    *   the closure to check.
    */
  private def getSerializedLambda(closure: AnyRef): Option[SerializedLambda] = {
    val isClosureCandidate =
      closure.getClass.isSynthetic &&
        closure.getClass.getInterfaces.exists(_.getName == "scala.Serializable")

    if (isClosureCandidate) {
      try {
        Option(IndylambdaScalaClosures.inspect(closure))
      } catch {
        case e: Exception =>
          logDebug("Closure is not a serialized lambda.", e)
          None
      }
    } else {
      None
    }
  }

  private def instantiateClass(cls: Class[_], enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }

  /** Sets accessed fields for given class in clone object based on given object. */
  private def setAccessedFields(
      outerClass: Class[_],
      clone: AnyRef,
      obj: AnyRef,
      accessedFields: Map[Class[_], Set[String]]
  ): Unit = {
    for (fieldName <- accessedFields(outerClass)) {
      val field = outerClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      val value = field.get(obj)
      field.set(clone, value)
    }
  }

  /** Clones a given object and sets accessed fields in cloned object. */
  private def cloneAndSetFields(
      parent: AnyRef,
      obj: AnyRef,
      outerClass: Class[_],
      accessedFields: Map[Class[_], Set[String]]
  ): AnyRef = {
    val clone = instantiateClass(outerClass, parent)

    var currentClass = outerClass
    assert(currentClass != null, "The outer class can't be null.")

    while (currentClass != null) {
      setAccessedFields(currentClass, clone, obj, accessedFields)
      currentClass = currentClass.getSuperclass()
    }

    clone
  }

  /** Clean the given closure in place. The mechanism is to traverse the hierarchy of enclosing
    * closures and null out any references along the way that are not actually used by the starting
    * closure, but are nevertheless included in the compiled anonymous classes.
    *
    * Closures are cleaned transitively. Does not verify whether the closure is serializable after
    * cleaning.
    *
    * @param func
    *   the closure to be cleaned
    * @param closureCleanerMode
    *   closure cleaner mode, can be always, never, repl_only.
    */
  private[snowpark] def clean(func: AnyRef, closureCleanerMode: ClosureCleanerMode.Value): Unit = {
    if (func == null || closureCleanerMode == ClosureCleanerMode.never) {
      return
    }
    val maybeIndylambdaProxy = getSerializedLambda(func)
    if (maybeIndylambdaProxy.isEmpty) {
      // Throw error, lambdaFunc should not be empty in scala 2.12.x
      throw ErrorMessage.UDF_NEED_SCALA_2_12_OR_LAMBDAFYMETHOD()
    }

    val lambdaProxy = maybeIndylambdaProxy.get
    val implMethodName = lambdaProxy.getImplMethodName

    logDebug(s"Cleaning indylambda closure: $implMethodName")

    // capturing class is the class that declared this lambda
    val capturingClassName = lambdaProxy.getCapturingClass.replace('/', '.')
    val classLoader = func.getClass.getClassLoader // this is the safest option
    // scalastyle:off classforname
    val capturingClass = Class.forName(capturingClassName, false, classLoader)
    // scalastyle:on classforname

    // Fail fast if we detect return statements in closures
    val capturingClassReader = getClassReader(capturingClass)
    capturingClassReader.accept(new ReturnStatementFinder(Option(implMethodName)), 0)

    // identify scala repl, vscode notebook, and Jupyter notebook
    // ------- added by Snowpark ------- //
    val isClosureInRepl = capturingClassName.startsWith("$line") ||
      (capturingClassName.startsWith("repl/") && capturingClassName.endsWith("$App")) ||
      capturingClassName.startsWith("ammonite")
    // ------- end ------- //
    val outerThisOpt = if (lambdaProxy.getCapturedArgCount > 0) {
      Option(lambdaProxy.getCapturedArg(0))
    } else {
      None
    }

    // only need to clean when there is an enclosing "this" captured by the closure, and it
    // should be something cleanable, i.e. a Scala REPL line object
    val needsCleaning = (closureCleanerMode == ClosureCleanerMode.always || isClosureInRepl) &&
      outerThisOpt.isDefined && outerThisOpt.get.getClass.getName == capturingClassName

    logDebug(s"capturingClassName: $capturingClassName")

    if (needsCleaning) {
      // indylambda closures do not reference enclosing closures via an `$outer` chain, so no
      // transitive cleaning on the `$outer` chain is needed.
      // Thus clean() shouldn't be recursively called with a non-empty accessedFields.
      val accessedFields: mutable.Map[Class[_], mutable.Set[String]] = mutable.Map.empty

      initAccessedFields(accessedFields, Seq(capturingClass))
      IndylambdaScalaClosures.findAccessedFields(
        lambdaProxy,
        classLoader,
        accessedFields,
        findTransitively = true
      )

      logDebug(s" + fields accessed by starting closure: ${accessedFields.size} classes")
      accessedFields.foreach { f =>
        logDebug("     " + f)
      }

      if (accessedFields(capturingClass).size < capturingClass.getDeclaredFields.length) {
        // clone and clean the enclosing `this` only when there are fields to null out

        val outerThis = outerThisOpt.get

        logDebug(s" + cloning instance of REPL class $capturingClassName")
        val clonedOuterThis =
          cloneAndSetFields(parent = null, outerThis, capturingClass, accessedFields)

        val outerField = func.getClass.getDeclaredField("arg$1")
        outerField.setAccessible(true)
        outerField.set(func, clonedOuterThis)
      }
      logDebug(s" +++ indylambda closure ($implMethodName) is now cleaned +++")
    } else {
      logDebug(s" +++ skipped cleaning indylambda closure ($implMethodName) +++")
    }

  }

  /** Initializes the accessed fields for outer classes and their super classes. */
  private def initAccessedFields(
      accessedFields: Map[Class[_], Set[String]],
      outerClasses: Seq[Class[_]]
  ): Unit = {
    for (cls <- outerClasses) {
      var currentClass = cls
      assert(currentClass != null, "The outer class can't be null.")

      while (currentClass != null) {
        accessedFields(currentClass) = Set.empty[String]
        currentClass = currentClass.getSuperclass()
      }
    }
  }

}

private object IndylambdaScalaClosures extends Logging {
  // internal name of java.lang.invoke.LambdaMetafactory
  val LambdaMetafactoryClassName = "java/lang/invoke/LambdaMetafactory"
  // the method that Scala indylambda use for bootstrap method
  val LambdaMetafactoryMethodName = "altMetafactory"
  val LambdaMetafactoryMethodDesc = "(Ljava/lang/invoke/MethodHandles$Lookup;" +
    "Ljava/lang/String;Ljava/lang/invoke/MethodType;[Ljava/lang/Object;)" +
    "Ljava/lang/invoke/CallSite;"

  def inspect(closure: AnyRef): SerializedLambda = {
    val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    writeReplace.invoke(closure).asInstanceOf[SerializedLambda]
  }

  /** Check if the handle represents the LambdaMetafactory that indylambda Scala closures use for
    * creating the lambda class and getting a closure instance.
    */
  def isLambdaMetafactory(bsmHandle: Handle): Boolean = {
    bsmHandle.getOwner == LambdaMetafactoryClassName &&
    bsmHandle.getName == LambdaMetafactoryMethodName &&
    bsmHandle.getDesc == LambdaMetafactoryMethodDesc
  }

  /** Check if the handle represents a target method that is:
    *   - a STATIC method that implements a Scala lambda body in the indylambda style
    *   - captures the enclosing `this`, i.e. the first argument is a reference to the same type as
    *     the owning class. Returns true if both criteria above are met.
    */
  def isLambdaBodyCapturingOuter(handle: Handle, ownerInternalName: String): Boolean = {
    handle.getTag == H_INVOKESTATIC &&
    handle.getName.contains("$anonfun$") &&
    handle.getOwner == ownerInternalName &&
    handle.getDesc.startsWith(s"(L$ownerInternalName;")
  }

  /** Check if the callee of a call site is a inner class constructor.
    *   - A constructor has to be invoked via INVOKESPECIAL
    *   - A constructor's internal name is "&lt;init&gt;" and the return type is "V" (void)
    *   - An inner class' first argument in the signature has to be a reference to the enclosing
    *     "this", aka `$outer` in Scala.
    */
  def isInnerClassCtorCapturingOuter(
      op: Int,
      owner: String,
      name: String,
      desc: String,
      callerInternalName: String
  ): Boolean = {
    op == INVOKESPECIAL && name == "<init>" && desc.startsWith(s"(L$callerInternalName;")
  }

  def findAccessedFields(
      lambdaProxy: SerializedLambda,
      lambdaClassLoader: ClassLoader,
      accessedFields: Map[Class[_], Set[String]],
      findTransitively: Boolean
  ): Unit = {

    // We may need to visit the same class multiple times for different methods on it, and we'll
    // need to lookup by name. So we use ASM's Tree API and cache the ClassNode/MethodNode.
    val classInfoByInternalName = Map.empty[String, (Class[_], ClassNode)]
    val methodNodeById = Map.empty[MethodIdentifier[_], MethodNode]

    // Recursively find all super class of the give class and add all of their method to the map.
    // ------- added by Snowpark ------- //
    def updateMethodMap(clazz: Class[_], initialClazz: Class[_]): (Class[_], ClassNode) = {
      if (clazz != null) {
        val classNode = new ClassNode()
        val classReader = ClosureCleaner.getClassReader(clazz)
        classReader.accept(classNode, 0)

        for (m <- classNode.methods.asScala) {
          methodNodeById(MethodIdentifier(initialClazz, m.name, m.desc)) = m
        }
        updateMethodMap(clazz.getSuperclass, initialClazz)

        (clazz, classNode)
      } else {
        (null, null)
      }
    }
    // ------- end ------- //
    def getOrUpdateClassInfo(classInternalName: String): (Class[_], ClassNode) = {
      val classInfo = classInfoByInternalName.getOrElseUpdate(
        classInternalName, {
          val classExternalName = classInternalName.replace('/', '.')
          // scalastyle:off classforname
          val clazz = Class.forName(classExternalName, false, lambdaClassLoader)
          // scalastyle:on classforname

          // This change is used to add methods of the super-classes to methodNodeById map.
          // Without this change, if the closure accessed any method of its super-classes,
          // we will have key not found error.
          // ------- added by Snowpark ------- //
          updateMethodMap(clazz, clazz)
          // ------- end ------- //
        }
      )
      classInfo
    }

    val implClassInternalName = lambdaProxy.getImplClass
    val (implClass, _) = getOrUpdateClassInfo(implClassInternalName)

    val implMethodId =
      MethodIdentifier(implClass, lambdaProxy.getImplMethodName, lambdaProxy.getImplMethodSignature)

    // The set internal names of classes that we would consider following the calls into.
    // Candidates are: known outer class which happens to be the starting closure's impl class,
    // and all inner classes discovered below.
    // Note that code in an inner class can make calls to methods in any of its enclosing classes,
    // e.g.
    //   starting closure (in class T)
    //     inner class A
    //        inner class B
    //          inner closure
    // we need to track calls from "inner closure" to outer classes relative to it (class T, A, B)
    // to better find and track field accesses.
    val trackedClassInternalNames = Set[String](implClassInternalName)

    // Depth-first search for inner closures and track the fields that were accessed in them.
    // Start from the lambda body's implementation method, follow method invocations
    val visited = Set.empty[MethodIdentifier[_]]
    val stack = Stack[MethodIdentifier[_]](implMethodId)
    def pushIfNotVisited(methodId: MethodIdentifier[_]): Unit = {
      if (!visited.contains(methodId)) {
        stack.push(methodId)
      }
    }

    while (!stack.isEmpty) {
      val currentId = stack.pop
      visited += currentId

      val currentClass = currentId.cls
      val currentMethodNode = methodNodeById(currentId)
      logTrace(s"  scanning ${currentId.cls.getName}.${currentId.name}${currentId.desc}")
      currentMethodNode.accept(new MethodVisitor(ASM7) {
        val currentClassName = currentClass.getName
        val currentClassInternalName = currentClassName.replace('.', '/')

        // Find and update the accessedFields info. Only fields on known outer classes are tracked.
        // This is the FieldAccessFinder equivalent.
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit = {
          if (op == GETFIELD || op == PUTFIELD) {
            val ownerExternalName = owner.replace('/', '.')
            for (cl <- accessedFields.keys if cl.getName == ownerExternalName) {
              logTrace(s"    found field access $name on $ownerExternalName")
              accessedFields(cl) += name
            }
          }
        }

        override def visitMethodInsn(
            op: Int,
            owner: String,
            name: String,
            desc: String,
            itf: Boolean
        ): Unit = {
          val ownerExternalName = owner.replace('/', '.')
          if (owner == currentClassInternalName) {
            logTrace(s"    found intra class call to $ownerExternalName.$name$desc")
            // could be invoking a helper method or a field accessor method, just follow it.
            pushIfNotVisited(MethodIdentifier(currentClass, name, desc))
          } else if (
            isInnerClassCtorCapturingOuter(op, owner, name, desc, currentClassInternalName)
          ) {
            // Discover inner classes.
            // This this the InnerClassFinder equivalent for inner classes, which still use the
            // `$outer` chain. So this is NOT controlled by the `findTransitively` flag.
            logDebug(s"    found inner class $ownerExternalName")
            val innerClassInfo = getOrUpdateClassInfo(owner)
            val innerClass = innerClassInfo._1
            val innerClassNode = innerClassInfo._2
            trackedClassInternalNames += owner
            // We need to visit all methods on the inner class so that we don't missing anything.
            for (m <- innerClassNode.methods.asScala) {
              logTrace(s"      found method ${m.name}${m.desc}")
              pushIfNotVisited(MethodIdentifier(innerClass, m.name, m.desc))
            }
          } else if (findTransitively && trackedClassInternalNames.contains(owner)) {
            logTrace(s"    found call to outer $ownerExternalName.$name$desc")
            val (calleeClass, _) = getOrUpdateClassInfo(owner) // make sure MethodNodes are cached
            pushIfNotVisited(MethodIdentifier(calleeClass, name, desc))
          } else {
            // keep the same behavior as the original ClosureCleaner
            logTrace(s"    ignoring call to $ownerExternalName.$name$desc")
          }
        }

        // Find the lexically nested closures
        // This is the InnerClosureFinder equivalent for indylambda nested closures
        override def visitInvokeDynamicInsn(
            name: String,
            desc: String,
            bsmHandle: Handle,
            bsmArgs: Object*
        ): Unit = {
          logTrace(s"    invokedynamic: $name$desc, bsmHandle=$bsmHandle, bsmArgs=$bsmArgs")

          // fast check: we only care about Scala lambda creation
          // TODO: maybe lift this restriction and support other functional interfaces
          if (!name.startsWith("apply")) return
          if (!Type.getReturnType(desc).getDescriptor.startsWith("Lscala/Function")) return

          if (isLambdaMetafactory(bsmHandle)) {
            // OK we're in the right bootstrap method for serializable Java 8 style lambda creation
            val targetHandle = bsmArgs(1).asInstanceOf[Handle]
            if (isLambdaBodyCapturingOuter(targetHandle, currentClassInternalName)) {
              // this is a lexically nested closure that also captures the enclosing `this`
              logDebug(s"    found inner closure $targetHandle")
              val calleeMethodId =
                MethodIdentifier(currentClass, targetHandle.getName, targetHandle.getDesc)
              pushIfNotVisited(calleeMethodId)
            }
          }
        }
      })
    }
  }
}

/** Helper class to identify a method. */
private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

private class ReturnStatementFinder(targetMethodName: Option[String] = None)
    extends ClassVisitor(ASM7) {
  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor = {

    // $anonfun$ covers indylambda closures
    if (name.contains("apply") || name.contains("$anonfun$")) {
      // A method with suffix "$adapted" will be generated in cases like
      // { _:Int => return; Seq()} but not { _:Int => return; true}
      // closure passed is $anonfun$t$1$adapted while actual code resides in $anonfun$s$1
      // visitor will see only $anonfun$s$1$adapted, so we remove the suffix, see
      // https://github.com/scala/scala-dev/issues/109
      val isTargetMethod = targetMethodName.isEmpty ||
        name == targetMethodName.get || name == targetMethodName.get.stripSuffix("$adapted")

      new MethodVisitor(ASM7) {
        override def visitTypeInsn(op: Int, tp: String): Unit = {
          if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl") && isTargetMethod) {
            throw ErrorMessage.UDF_RETURN_STATEMENT_IN_CLOSURE_EXCEPTION()
          }
        }
      }
    } else {
      new MethodVisitor(ASM7) {}
    }
  }
}
