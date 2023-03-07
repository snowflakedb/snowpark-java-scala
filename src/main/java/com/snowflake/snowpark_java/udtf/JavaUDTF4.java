package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 4 arguments. */
public interface JavaUDTF4<A0, A1, A2, A3> extends JavaUDTF {
  Stream<Row> process(A0 arg0, A1 arg1, A2 arg2, A3 arg3);
}
