package com.snowflake.snowpark;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaUtilsSuite {

  @Test
  public void javaStringMapToScala() {
    Map<String, Object> map = new HashMap<>();
    map.put("a", null);
    assert JavaUtils.javaStringAnyMapToScala(map).get("a").get() == null;
  }
}
