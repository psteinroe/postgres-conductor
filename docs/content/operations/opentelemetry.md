# OpenTelemetry

Tracing is enabled by default and uses the application's global OpenTelemetry API. Install and configure the OpenTelemetry SDK in the application; the application owns exporters and shutdown.

Set `telemetry: false` on the Conductor to opt out. Each execution stores one trace-state envelope with an optional parent and one-shot event link. Each carrier contains only bounded W3C trace context (`traceparent` and optional `tracestate`); payloads, baggage, and arbitrary propagator fields are never recorded or propagated.

Producer and consumer spans preserve trace topology across direct invocation, child tasks, cron schedules, events, event waits, and dead-letter delivery. Messaging attributes follow the experimental OpenTelemetry messaging semantic conventions.

pgconductor does not emit standard metrics. Application-specific metrics need different dimensions and cardinality controls, so lifecycle hooks are a better extension point than a fixed built-in metric set. Those hooks will be designed separately from tracing.
