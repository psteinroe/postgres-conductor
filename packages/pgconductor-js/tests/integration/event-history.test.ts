import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const INTERNAL_QUEUE = "pgconductor.internal";

const message = defineEvent({
	name: "history.message",
	payload: z.object({ channel: z.string(), thread: z.string(), text: z.string() }),
});
const other = defineEvent({
	name: "history.other",
	payload: z.object({ channel: z.string() }),
});

describe("event history", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function setup(eventRetention?: { days: number }) {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			events: [message, other],
			eventRetention,
			context: {},
		});
		await conductor.ensureInstalled();
		return { db, conductor };
	}

	test("reads kept events by name and payload fields, in the order they were stored", async () => {
		const { conductor } = await setup();
		const ids: string[] = [];
		for (const [thread, text] of [
			["t1", "one"],
			["t2", "two"],
			["t1", "three"],
			["t1", "four"],
		] as const) {
			ids.push(await conductor.emit("history.message", { channel: "C1", thread, text }));
		}
		await conductor.emit("history.other", { channel: "C1" });

		const thread = await conductor.listEvents({ name: "history.message", where: { thread: "t1" } });
		expect(thread.map((event) => event.payload.text)).toEqual(["one", "three", "four"]);
		expect(thread[0]).toMatchObject({
			id: ids[0],
			name: "history.message",
			payload: { channel: "C1", thread: "t1", text: "one" },
		});
		expect(thread[0]?.createdAt).toBeInstanceOf(Date);

		expect(await conductor.countEvents({ name: "history.message", where: { thread: "t1" } })).toBe(
			3,
		);
		expect(await conductor.countEvents({ name: "history.message" })).toBe(4);
		expect(await conductor.countEvents({ name: "history.other" })).toBe(1);

		const newest = await conductor.listEvents({
			name: "history.message",
			where: { thread: "t1" },
			order: "desc",
			limit: 2,
		});
		expect(newest.map((event) => event.payload.text)).toEqual(["four", "three"]);

		const page = await conductor.listEvents({
			name: "history.message",
			where: { thread: "t1" },
			offset: 1,
			limit: 1,
		});
		expect(page.map((event) => event.payload.text)).toEqual(["three"]);

		// The events next to "three" on its thread.
		const [before] = await conductor.listEvents({
			name: "history.message",
			where: { thread: "t1" },
			before: ids[2],
			order: "desc",
			limit: 1,
		});
		const [after] = await conductor.listEvents({
			name: "history.message",
			where: { thread: "t1" },
			after: ids[2],
			limit: 1,
		});
		expect(before?.payload.text).toBe("one");
		expect(after?.payload.text).toBe("four");

		const event = await conductor.getEvent(ids[1] || "");
		expect(event).toMatchObject({ id: ids[1], name: "history.message", payload: { text: "two" } });
		expect(await conductor.getEvent("00000000-0000-7000-8000-000000000000")).toBeNull();
	});

	test("reads events stored after a time", async () => {
		const { db, conductor } = await setup();
		try {
			await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
			await conductor.emit("history.other", { channel: "old" });
			await db.client.setFakeTime({ date: new Date("2024-01-01T13:00:00Z") });
			await conductor.emit("history.other", { channel: "new" });
		} finally {
			await db.client.clearFakeTime();
		}
		const since = await conductor.listEvents({
			name: "history.other",
			since: new Date("2024-01-01T12:30:00Z"),
		});
		expect(since.map((event) => event.payload.channel)).toEqual(["new"]);
	});

	test("eventRetention keeps dispatched events, and their dedupe keys, for as long", async () => {
		const { db, conductor } = await setup({ days: 30 });
		const orchestrator = Orchestrator.create({
			conductor,
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		try {
			await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
			const id = await conductor.emit("history.other", { channel: "C1" }, { dedupe_key: "d1" });
			await orchestrator.drain();

			await db.client.setFakeTime({ date: new Date("2024-01-20T12:00:00Z") });
			await db.client.removeExecutions({ queueName: INTERNAL_QUEUE, batchSize: 100 });
			expect(await conductor.getEvent(id)).not.toBeNull();
			expect(await conductor.emit("history.other", { channel: "C1" }, { dedupe_key: "d1" })).toBe(
				id,
			);

			await db.client.setFakeTime({ date: new Date("2024-02-01T13:00:00Z") });
			await db.client.removeExecutions({ queueName: INTERNAL_QUEUE, batchSize: 100 });
			expect(await conductor.getEvent(id)).toBeNull();
			expect(await conductor.countEvents({ name: "history.other" })).toBe(0);
		} finally {
			await db.client.clearFakeTime();
		}
	});

	test("a triggered task gets the id of the event it handles", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({ name: "history.handler", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			events: [other],
			context: {},
		});
		await conductor.ensureInstalled();
		const received: string[] = [];
		const task = conductor.createTask(
			{ name: "history.handler" },
			{ event: "history.other" },
			async (event) => {
				received.push(event.id);
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		try {
			const id = await conductor.emit("history.other", { channel: "C1" });
			await waitForCondition(() => received.length === 1);
			expect(received).toEqual([id]);
			expect((await conductor.getEvent(id))?.payload).toEqual({ channel: "C1" });
		} finally {
			await orchestrator.stop();
		}
	});

	test("reads events through the partial event indexes", async () => {
		const { db, conductor } = await setup();
		await conductor.emit("history.message", { channel: "C1", thread: "t1", text: "one" });
		await db.sql`set enable_seqscan = off`;
		const plan = await db.sql.unsafe<{ "QUERY PLAN": string }[]>(`
			explain select id from pgconductor._private_executions
			where queue = 'pgconductor.internal' and task_key = 'pgconductor.event-dispatch'
				and payload->>'eventKey' = 'history.message'
				and payload->'payload' @> '{"thread":"t1"}'::jsonb
			order by created_at, id
		`);
		expect(plan.map((row) => row["QUERY PLAN"]).join("\n")).toMatch(/Index|Bitmap/);
	});
});
