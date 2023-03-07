package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 8 arguments. */
public interface JavaUDTF8<A0, A1, A2, A3, A4, A5, A6, A7> extends JavaUDTF {
  Stream<Row> process(A0 arg0, A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6, A7 arg7);
}
