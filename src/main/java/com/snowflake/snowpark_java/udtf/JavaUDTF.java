package com.snowflake.snowpark_java.udtf;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.*;
import java.io.Serializable;
import java.util.stream.Stream;

/** The base interface for JavaUDTF[N]. */
public interface JavaUDTF extends Serializable {
  StructType outputSchema();

  default StructType inputSchema() {
    return StructType.create();
  }

  Stream<Row> endPartition();
}
