package com.snowflake.snowpark_java.udaf;

import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.StructType;
import java.io.Serializable;

/** The base interface for JavaUDAF[N]. */
public interface JavaUDAF extends Serializable {
  DataType outputType();

  default StructType inputSchema() {
    return StructType.create();
  }
}

// Code to generate UDAF interfaces
// (1 to 22).foreach { x =>
//   val typeParams = (0 until x).map(i => s"A$i").mkString(", ")
//   val accArgs = (0 until x).map(i => s"A$i arg$i").mkString(", ")
//   println(s"""
//     |package com.snowflake.snowpark_java.udaf;
//     |
//     |public interface JavaUDAF$x<S, O, $typeParams> extends JavaUDAF {
//     |  S initialize();
//     |
//     |  S accumulate(S state, $accArgs);
//     |
//     |  S merge(S state1, S state2);
//     |
//     |  O terminate(S state);
//     |}""".stripMargin)
// }
