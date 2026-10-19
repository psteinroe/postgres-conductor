import { describe, expect, mock, test } from "bun:test";
import type { StoredSignal } from "../../src/database-client";
import { SignalInbox } from "../../src/signal-inbox";
import { Deferred } from "../../src/lib/deferred";

function store() {
	const signals: StoredSignal[] = [];
	const acked: string[] = [];
	return {
		signals,
		acked,
		add(name: string, id: string) {
			signals.push({ seq: signals.length + 1, id, name, payload: id });
		},
		/** what a worker poll reads for an inbox */
		poll(inbox: SignalInbox) {
			const afterSeq = inbox.afterSeq;
			if (afterSeq === null) return;
			inbox.push(signals.filter((s) => s.seq > afterSeq && !acked.includes(s.id)));
		},
		ack: async (ids: string[]) => {
			acked.push(...ids);
		},
	};
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("SignalInbox", () => {
	test("is not polled until a handler is registered or after it closed", async () => {
		const inbox = new SignalInbox({ ack: async () => {}, onError: () => {} });
		expect(inbox.afterSeq).toBeNull();
		inbox.listen("input", () => {});
		expect(inbox.afterSeq).toBe(0);
		await inbox.close({ wait: true });
		expect(inbox.afterSeq).toBeNull();
	});

	test("delivers stored signals in order and acknowledges each after its handler", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		inbox.listen("input", (_payload, { id }) => {
			expect(s.acked).toEqual(delivered);
			delivered.push(id);
		});
		s.poll(inbox);
		await flush();
		s.add("input", "c");
		s.poll(inbox);
		await inbox.close({ wait: true });

		expect(delivered).toEqual(["a", "b", "c"]);
		expect(s.acked).toEqual(["a", "b", "c"]);
	});

	test("keeps signals without a handler until one registers", async () => {
		const s = store();
		s.add("later", "l1");
		s.add("input", "i1");
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		inbox.listen("input", (_payload, { id }) => delivered.push(id));
		s.poll(inbox);
		await flush();
		expect(delivered).toEqual(["i1"]);

		inbox.listen("later", (_payload, { id }) => delivered.push(id));
		s.poll(inbox);
		await flush();
		await inbox.close({ wait: true });
		expect(delivered).toEqual(["i1", "l1"]);
		expect(inbox.names).toEqual(["input", "later"]);
	});

	test("runs one handler at a time", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const release = new Deferred<void>();
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		inbox.listen("input", async (_payload, { id }) => {
			delivered.push(id);
			await release.promise;
		});
		s.poll(inbox);
		await flush();
		expect(delivered).toEqual(["a"]);
		release.resolve();
		await flush();
		await inbox.close({ wait: true });
		expect(delivered).toEqual(["a", "b"]);
	});

	test("a throwing handler reports an error and leaves its signal unacknowledged", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const onError = mock((_error: Error) => {});
		const inbox = new SignalInbox({ ack: s.ack, onError });

		inbox.listen("input", () => {
			throw new Error("boom");
		});
		s.poll(inbox);
		await flush();
		await flush();

		expect(onError).toHaveBeenCalledTimes(1);
		expect(onError.mock.calls[0]?.[0].message).toBe('Signal handler for "input" failed: boom');
		expect(s.acked).toEqual([]);
		expect((await inbox.close({ wait: true }))?.message).toContain("boom");
	});

	test("close waits for the running handler and stops further deliveries", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const release = new Deferred<void>();
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		inbox.listen("input", async (_payload, { id }) => {
			delivered.push(id);
			await release.promise;
		});
		s.poll(inbox);
		await flush();
		const closed = inbox.close({ wait: true });
		release.resolve();
		expect(await closed).toBeNull();

		expect(delivered).toEqual(["a"]);
		expect(s.acked).toEqual(["a"]);
		expect(() => inbox.listen("other", () => {})).toThrow();
	});

	test("rejects a second handler for a name", () => {
		const inbox = new SignalInbox({ ack: async () => {}, onError: () => {} });
		inbox.listen("input", () => {});
		expect(() => inbox.listen("input", () => {})).toThrow(/already registered/);
	});
});
