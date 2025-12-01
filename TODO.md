- prepare demo/
- write README.md
- write docs/

- separate events part, maybe even handle migrations individually
- only use custom current_time if running tests
- otel integration
- add management ui
- iterate on batched processing: allow array payloads and if configured, invoke task with all its executions found in the local queue?
- add throttling (limit, period, key)
- add concurrency (limit, key)
- add rateLimit (limit, period, key)
- add debounce (period, key)

Links:
- https://planetscale.com/blog/the-slotted-counter-pattern

