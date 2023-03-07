package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 0 argument. */
@FunctionalInterface
public interface JavaSProc0<RT> extends JavaSProc {
  RT call(Session session);
}
// Code to generate SProc interfaces
// (0 to 21).foreach { x =>
//        val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
//        val args = if (x == 0) "" else
//        (1 to x).map(i => s"A$i arg$i").mkString(", ", ", ", "")
//        println(s"""
//    |package com.snowflake.snowpark_java.sproc;
//    |
//    |import com.snowflake.snowpark_java.Session;
//    |import com.snowflake.snowpark_java.internal.JavaSProc;
//    |
//    |/** A Java SProc interface that has $x argument${if(x>1) "s" else ""}. */
//    |@FunctionalInterface
//    |public interface JavaSProc$x<$types> extends JavaSProc {
//    |  RT call(Session session$args);
//    |}""".stripMargin)
// }
