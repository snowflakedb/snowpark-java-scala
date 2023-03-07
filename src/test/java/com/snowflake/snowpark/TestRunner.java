package com.snowflake.snowpark;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestRunner {
  private static final JUnitCore junit = init();

  private static JUnitCore init() {
    // turn on assertion, otherwise Java assert doesn't work.
    TestRunner.class.getClassLoader().setDefaultAssertionStatus(true);
    JUnitCore jCore = new JUnitCore();
    jCore.addListener(new TextListener(System.out));
    return jCore;
  }

  public static void run(Class<?> clazz) {
    Result result = junit.run(clazz);

    StringBuilder failures = new StringBuilder();
    for (Failure f : result.getFailures()) {
      failures.append(f.getTrimmedTrace());
    }

    System.out.println(
        "Finished. Result: Failures: "
            + result.getFailureCount()
            + " Ignored: "
            + result.getIgnoreCount()
            + " Tests run: "
            + result.getRunCount()
            + ". Time: "
            + result.getRunTime()
            + "ms.");
    if (result.getFailureCount() > 0) {
      throw new RuntimeException("Failures:\n" + failures.toString());
    }
  }
}
