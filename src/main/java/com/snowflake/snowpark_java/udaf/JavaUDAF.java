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
