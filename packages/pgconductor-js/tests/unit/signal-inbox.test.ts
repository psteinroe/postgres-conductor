import { describe, expect, test, vi } from "vitest";
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
			for (const signal of signals) {
				if (signal.seq > afterSeq && !acked.includes(signal.id)) inbox.push(signal);
			}
		},
		ack: async (id: string) => {
			acked.push(id);
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
		await inbox.close({ drain: true });
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
		await inbox.close({ drain: true });

		expect(delivered).toEqual(["a", "b", "c"]);
		expect(s.acked).toEqual(["a", "b", "c"]);
	});

	test("skips signals without a handler and leaves them unacknowledged", async () => {
		const s = store();
		s.add("later", "l1");
		s.add("input", "i1");
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		inbox.listen("input", (_payload, { id }) => delivered.push(id));
		s.poll(inbox);
		await flush();
		inbox.listen("later", (_payload, { id }) => delivered.push(id));
		s.poll(inbox);
		await inbox.close({ drain: true });

		// settlement re-queues the execution for l1, since this attempt listened for its name
		expect(delivered).toEqual(["i1"]);
		expect(s.acked).toEqual(["i1"]);
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
		await inbox.close({ drain: true });
		expect(delivered).toEqual(["a", "b"]);
	});

	test("a throwing handler reports an error and leaves its signal unacknowledged", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const onError = vi.fn((_error: Error) => {});
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
		expect((await inbox.close({ drain: true }))?.message).toContain("boom");
	});

	test("signals pushed before a handler registers are delivered when it does", async () => {
		const s = store();
		s.add("input", "a");
		s.add("input", "b");
		const delivered: string[] = [];
		const inbox = new SignalInbox({ ack: s.ack, onError: () => {} });

		// what the worker reads with the claim
		for (const signal of s.signals) inbox.push(signal);
		expect(inbox.afterSeq).toBeNull();
		inbox.listen("input", (_payload, { id }) => delivered.push(id));
		expect(inbox.afterSeq).toBe(2);
		// a poll does not read them again
		s.poll(inbox);
		await inbox.close({ drain: true });

		expect(delivered).toEqual(["a", "b"]);
		expect(s.acked).toEqual(["a", "b"]);
	});

	test("close with drain delivers the received signals and accepts no more", async () => {
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
		const closed = inbox.close({ drain: true });
		s.add("input", "c");
		for (const signal of s.signals) inbox.push(signal);
		release.resolve();
		expect(await closed).toBeNull();

		expect(delivered).toEqual(["a", "b"]);
		expect(s.acked).toEqual(["a", "b"]);
		expect(() => inbox.listen("other", () => {})).toThrow();
	});

	test("close without drain stops after the running handler", async () => {
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
		const closed = inbox.close({ drain: false });
		release.resolve();
		expect(await closed).toBeNull();

		expect(delivered).toEqual(["a"]);
		expect(s.acked).toEqual(["a"]);
	});

	test("a handler that throws while draining fails close without reporting an error", async () => {
		const s = store();
		s.add("input", "a");
		const onError = vi.fn((_error: Error) => {});
		const inbox = new SignalInbox({ ack: s.ack, onError });

		for (const signal of s.signals) inbox.push(signal);
		inbox.listen("input", async () => {
			await flush();
			throw new Error("boom");
		});

		expect((await inbox.close({ drain: true }))?.message).toBe(
			'Signal handler for "input" failed: boom',
		);
		expect(onError).not.toHaveBeenCalled();
		expect(s.acked).toEqual([]);
	});

	test("rejects a second handler for a name", () => {
		const inbox = new SignalInbox({ ack: async () => {}, onError: () => {} });
		inbox.listen("input", () => {});
		expect(() => inbox.listen("input", () => {})).toThrow(/already registered/);
	});
});
