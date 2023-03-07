package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 3 arguments. */
public interface JavaUDTF3<A0, A1, A2> extends JavaUDTF {
  Stream<Row> process(A0 arg0, A1 arg1, A2 arg2);
}
