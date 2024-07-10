package com.snowflake.snowpark

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.internal.data.ExceptionEventData

trait OpenTelemetryEnabled extends SNTestBase {
  lazy protected val testSpanExporter: InMemorySpanExporter = InMemorySpanExporter.create()

  override def beforeAll: Unit = {
    super.beforeAll
    GlobalOpenTelemetry.resetForTest()
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
      methodChain: String): Unit =
    checkSpan(className, funcName) { span =>
      {
        assert(span.getTotalAttributeCount == 3)
        assert(span.getAttributes.get(AttributeKey.stringKey("code.filepath")) == fileName)
        assert(span.getAttributes.get(AttributeKey.longKey("code.lineno")) == lineNumber)
        assert(span.getAttributes.get(AttributeKey.stringKey("method.chain")) == methodChain)
      }
    }

  def checkSpan(className: String, funcName: String)(func: SpanData => Unit): Unit = {
    val span: SpanData = testSpanExporter.getFinishedSpanItems.get(0)
    assert(span.getName == funcName)
    assert(span.getInstrumentationScopeInfo.getName == className)
    func(span)
    testSpanExporter.reset()
  }

  def checkSpanError(className: String, funcName: String, error: Throwable): Unit =
    checkSpan(className, funcName) { span =>
      {
        assert(span.getStatus.getStatusCode == StatusCode.ERROR)
        assert(span.getStatus.getDescription == error.getMessage)
        assert(span.getEvents.size() == 1)
        assert(span.getEvents.get(0).isInstanceOf[ExceptionEventData])
        assert(span.getEvents.get(0).asInstanceOf[ExceptionEventData].getException == error)
      }
    }

  override def afterAll: Unit = {
    GlobalOpenTelemetry.resetForTest()
    super.afterAll
  }
}
