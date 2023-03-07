package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 13 arguments. */
public interface JavaUDTF13<A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12>
    extends JavaUDTF {
  Stream<Row> process(
      A0 arg0,
      A1 arg1,
      A2 arg2,
      A3 arg3,
      A4 arg4,
      A5 arg5,
      A6 arg6,
      A7 arg7,
      A8 arg8,
      A9 arg9,
      A10 arg10,
      A11 arg11,
      A12 arg12);
}
