package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 0 arguments. */
public interface JavaUDTF0 extends JavaUDTF {
  Stream<Row> process();
}

//  test("generate JavaUDTF code") {
//    val targetPath = "/tmp/java"
//    (0 to 22) foreach { i =>
//      val hasArgs = if (i == 1) "has 1 argument" else s"has $i arguments"
//      val types = if (i > 0) (0 until i).map(j => s"A$j").mkString("<", ", ", ">") else ""
//      val args = if (i > 0) (0 until i).map(j => s"A$j arg$j").mkString(", ") else ""
//      val code = s"""package com.snowflake.snowpark_java.udtf;
//                   |
//                   |import com.snowflake.snowpark_java.Row;
//                   |import java.util.stream.Stream;
//                   |
//                   |/** A Java UDTF interface that $hasArgs. */
//                   |public interface JavaUDTF$i$types extends JavaUDTF {
//                   |  Stream<Row> process($args);
//                   |}
//                   |""".stripMargin
//      import java.io._
//      val pw = new PrintWriter(new File(s"$targetPath/JavaUDTF$i.java"))
//      pw.write(code)
//      pw.close()
//    }
//  }
