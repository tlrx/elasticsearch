/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.internal.grpc.CustomOkHttpGrpcExporter;
import io.opentelemetry.exporter.otlp.internal.grpc.GrpcExporter;
import io.opentelemetry.exporter.otlp.internal.traces.TraceRequestMarshaler;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import okhttp3.Dispatcher;
import okhttp3.Headers;
import okhttp3.OkHttpClient;

import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    public static final CapturingSpanExporter CAPTURING_SPAN_EXPORTER = new CapturingSpanExporter();

    private final Map<Long, Span> taskSpans = ConcurrentCollections.newConcurrentMap();
    private final ThreadPool threadPool;

    private volatile Tracer tracer;

    public APMTracer(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    protected void doStart() {
        final GrpcExporter<TraceRequestMarshaler> delegate = AccessController.doPrivileged(
            (PrivilegedAction<GrpcExporter<TraceRequestMarshaler>>) () -> {
                final OkHttpClient client = new OkHttpClient.Builder().dispatcher(new Dispatcher(threadPool.generic())).build();
                return new CustomOkHttpGrpcExporter<>(
                    "span",
                    client,
                    "https://<redacted>.apm.us-central1.gcp.foundit.no",
                    new Headers.Builder()
                        .add("Authorization", "redacted")
                        .build(),
                    false
                );
            }
        );

        // Basically what OtlpGrpcSpanExporter does, but not final
        final SpanExporter exporter = new SpanExporter() {

            @Override
            public CompletableResultCode export(Collection<SpanData> spans) {
                final TraceRequestMarshaler request = TraceRequestMarshaler.create(spans);
                return delegate.export(request, spans.size());
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                return delegate.shutdown();
            }
        };

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(
                SpanProcessor.composite(SimpleSpanProcessor.create(CAPTURING_SPAN_EXPORTER), SimpleSpanProcessor.create(exporter))
            )
            .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();

        tracer = openTelemetry.getTracer("elasticsearch", Version.CURRENT.toString());
        tracer.spanBuilder("startup").startSpan().end();
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public void onTaskRegistered(Task task) {
        final Tracer tracer = this.tracer;
        if (tracer != null) {
            taskSpans.computeIfAbsent(task.getId(), taskId -> {
                final Span span = tracer.spanBuilder(task.getAction()).startSpan();
                span.setAttribute("es.task.id", task.getId());
                return span;
            });
        }
    }

    @Override
    public void onTaskUnregistered(Task task) {
        final Span span = taskSpans.remove(task.getId());
        if (span != null) {
            span.end();
        }
    }

    public static class CapturingSpanExporter implements SpanExporter {

        private List<SpanData> capturedSpans = new ArrayList<>();

        public void clear() {
            capturedSpans.clear();
        }

        public List<SpanData> getCapturedSpans() {
            return List.copyOf(capturedSpans);
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            capturedSpans.addAll(spans);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }
}
