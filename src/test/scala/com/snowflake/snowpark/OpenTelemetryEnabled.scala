package com.snowflake.snowpark

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor

trait OpenTelemetryEnabled extends SNTestBase {
  lazy protected val testSpanExporter: InMemorySpanExporter = InMemorySpanExporter.create()

  override def beforeAll: Unit = {
    super.beforeAll
    val resource = Resource.getDefault.toBuilder
      .put("service.name", "test-server")
      .put("service.version", "0.1.0")
      .build()

    val sdkTracerProvider = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(testSpanExporter))
      .setResource(resource)
      .build()

    OpenTelemetrySdk
      .builder()
      .setTracerProvider(sdkTracerProvider)
      .buildAndRegisterGlobal()
  }

  def checkSpan(
      className: String,
      funcName: String,
      fileName: String,
      lineNumber: Int,
      methodChain: String): Unit = {
    val span = testSpanExporter.getFinishedSpanItems.get(0)
    assert(span.getTotalAttributeCount == 3) // code.filepath, code.lineno, method.chain
    assert(span.getName == funcName)
    assert(span.getInstrumentationScopeInfo.getName == className)
    assert(span.getAttributes.get(AttributeKey.stringKey("code.filepath")) == fileName)
    assert(span.getAttributes.get(AttributeKey.longKey("code.lineno")) == lineNumber)
    assert(span.getAttributes.get(AttributeKey.stringKey("method.chain")) == methodChain)
  }

  override def afterAll: Unit = {
    GlobalOpenTelemetry.resetForTest()
    super.afterAll
  }
}
