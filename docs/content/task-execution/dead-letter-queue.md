# Dead-letter queues

Configure a destination for executions that fail on their final attempt:

```ts
const failedPayment = conductor.createTask(
  { name: "failed-payment", queue: "payments-dlq" },
  { invocable: true },
  async (event) => {},
);

const chargeCard = conductor.createTask(
  {
    name: "charge-card",
    deadLetter: { queue: "payments-dlq", task: failedPayment },
  },
  { invocable: true },
  async (event) => {},
);
```

The destination is a new execution with the original payload. Its execution row contains machine-readable source execution ID, source queue and task, final error, attempt count, and failure timestamp. The source remains a normal failed execution unless its retention policy removes it.

Retries and cancellation do not deliver to a dead-letter queue. Delivery is transactional, claim-fenced, and idempotent. A destination may have its own retry, retention, and concurrency settings. Chains are supported, but a task cannot target itself directly. The destination task must accept the source payload.
