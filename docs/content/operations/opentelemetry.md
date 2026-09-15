# OpenTelemetry

Tracing and metrics are enabled by default and use the application's global OpenTelemetry API. A Node `NodeSDK` can therefore be installed before or after Conductor; the application owns exporters and shutdown.

Set `telemetry: false` on the Conductor to opt out. Propagation is bounded W3C trace context only: payloads and baggage are never recorded or propagated.

Producer, consumer, process, child, cron, event, wait-resume, and dead-letter boundaries preserve causal topology. Messaging attributes follow the current OpenTelemetry messaging convention (implementation status: experimental). Metrics use seconds and bounded queue/task/outcome dimensions.
