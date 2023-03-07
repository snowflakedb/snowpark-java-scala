package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 0 argument. */
@FunctionalInterface
public interface JavaUDF0<RT> extends JavaUDF {
  RT call();
}

// Code to generate UDF interfaces
// (0 to 22).foreach { x =>
//   val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
//   val args = (1 to x).map(i => s"A$i arg$i").mkString(", ")
//   println(s"""
//     |package com.snowflake.snowpark_java.udf;
//     |
//     |import com.snowflake.snowpark_java.internal.JavaUDF;
//     |
//     |/** A Java UDF interface that has $x argument${if(x>1) "s" else ""}. */
//     |@FunctionalInterface
//     |public interface JavaUDF$x<$types> extends JavaUDF {
//     |  RT call($args);
//     |}""".stripMargin)
// }
