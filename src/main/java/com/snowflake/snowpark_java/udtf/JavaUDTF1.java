package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import java.util.stream.Stream;

/** A Java UDTF interface that has 1 argument. */
public interface JavaUDTF1<A0> extends JavaUDTF {
  Stream<Row> process(A0 arg0);
}
