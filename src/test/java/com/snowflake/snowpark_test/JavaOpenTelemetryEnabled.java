package com.snowflake.snowpark_test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Objects;

public abstract class JavaOpenTelemetryEnabled extends TestBase {
  protected InMemorySpanExporter testSpanExporter = init();

  private InMemorySpanExporter init() {
    InMemorySpanExporter testExporter = InMemorySpanExporter.create();
    GlobalOpenTelemetry.resetForTest();
    Resource resource =
        Resource.getDefault()
            .toBuilder()
            .put("service.name", "test-server")
            .put("service.version", "0.1.0")
            .build();
    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(testExporter))
            .setResource(resource)
            .build();
    OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider).buildAndRegisterGlobal();
    return testExporter;
  }

  protected void checkSpan(
      String className, String funcName, String fileName, int lineNumber, String methodChain) {
    SpanData span = testSpanExporter.getFinishedSpanItems().get(0);
    assert span.getName().equals(funcName);
    assert span.getInstrumentationScopeInfo().getName().equals(className);
    assert span.getTotalAttributeCount() == 3;
    assert Objects.equals(
        span.getAttributes().get(AttributeKey.stringKey("code.filepath")), fileName);
    assert Objects.equals(
        span.getAttributes().get(AttributeKey.longKey("code.lineno")), (long) lineNumber);
    String a = span.getAttributes().get(AttributeKey.stringKey("method.chain"));
    System.out.println(a + "\n" + methodChain);
    System.out.println(a.equals(methodChain));
    assert Objects.equals(a, methodChain);
    testSpanExporter.reset();
  }
}
