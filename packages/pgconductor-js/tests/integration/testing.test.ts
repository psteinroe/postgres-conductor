// Uses only the public package API: the package check runs this file against the packed tarball
import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeEach, expect, test } from "vitest";
import { Conductor, Orchestrator, TaskSchemas, type DefineTask } from "pgconductor-js";
import { createTestClock, type TestClock } from "pgconductor-js/testing";

type Tasks =
	| DefineTask<{ name: "report" }>
	| DefineTask<{ name: "remind"; payload: { to: string } }>
	| DefineTask<{ name: "charge"; returns: { attempt: number } }>;

const serverUrl =
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const server = postgres(serverUrl, { max: 1, onnotice: () => {} });

let database: string;
let sql: Sql;
let clock: TestClock;

beforeEach(async () => {
	database = `pgconductor_test_${randomUUID().replaceAll("-", "")}`;
	await server.unsafe(`create database ${database}`);
	const url = new URL(serverUrl);
	url.pathname = `/${database}`;
	// Fake time is a session setting, so everything shares one connection
	sql = postgres(url.toString(), { max: 1 });
	clock = createTestClock(sql);
	await createConductor().ensureInstalled();
	await clock.set(new Date("2024-01-01T12:00:00Z"));
});

afterEach(async () => {
	await sql.end();
	await server.unsafe(`drop database ${database}`);
});

afterAll(async () => {
	await server.end();
});

function createConductor() {
	return Conductor.create({
		sql,
		tasks: TaskSchemas.fromUnion<Tasks>(),
		context: {},
		clock,
	});
}

test("runs a cron task when time advances", async () => {
	const conductor = createConductor();
	let runs = 0;
	const report = conductor.createTask(
		{ name: "report" },
		{ cron: "*/15 * * * *", name: "quarter-hourly" },
		async () => {
			runs++;
		},
	);
	const orchestrator = Orchestrator.create({ conductor, tasks: [report] });

	// Schedules 12:15
	await orchestrator.drain();
	expect(runs).toBe(0);

	await clock.advance("15m");
	await orchestrator.drain();
	expect(runs).toBe(1);

	await clock.advance("15m");
	await orchestrator.drain();
	expect(runs).toBe(2);
});

test("resumes ctx.sleep when time advances", async () => {
	const conductor = createConductor();
	const sent: string[] = [];
	const remind = conductor.createTask(
		{ name: "remind" },
		{ invocable: true },
		async (event, ctx) => {
			await ctx.sleep("wait", 60 * 60 * 1000);
			sent.push(event.payload.to);
		},
	);
	const orchestrator = Orchestrator.create({ conductor, tasks: [remind] });

	const id = await conductor.invoke({ name: "remind" }, { to: "ada@example.com" });
	await orchestrator.drain();
	expect(sent).toEqual([]);

	await clock.advance("1h");
	await orchestrator.drain();
	expect(sent).toEqual(["ada@example.com"]);
	expect(await conductor.getExecution(id)).toMatchObject({
		status: "completed",
		completedAt: new Date("2024-01-01T13:00:00Z"),
	});
});

test("retries after the backoff", async () => {
	const conductor = createConductor();
	let attempts = 0;
	const charge = conductor.createTask({ name: "charge" }, { invocable: true }, async () => {
		attempts++;
		if (attempts === 1) throw new Error("card declined");
		return { attempt: attempts };
	});
	const orchestrator = Orchestrator.create({ conductor, tasks: [charge] });

	const id = await conductor.invoke({ name: "charge" }, {});
	await orchestrator.drain();
	expect(await conductor.getExecution(id)).toMatchObject({ status: "pending", attempts: 1 });

	// The first retry waits 15 seconds
	await clock.advance("14s");
	await orchestrator.drain();
	expect(attempts).toBe(1);

	await clock.advance("1s");
	await orchestrator.drain();
	expect(await conductor.getExecution({ name: "charge" }, id)).toMatchObject({
		status: "completed",
		result: { attempt: 2 },
	});
});

test("running workers follow clock changes", async () => {
	const conductor = createConductor();
	let runs = 0;
	const report = conductor.createTask(
		{ name: "report" },
		{ cron: "*/15 * * * *", name: "quarter-hourly" },
		async () => {
			runs++;
		},
	);
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [report],
		defaultWorker: { pollIntervalMs: 20 },
	});
	await orchestrator.start();

	await clock.advance("15m");
	await expect.poll(() => runs).toBe(1);

	// The next run is scheduled from the new time, at 12:30
	await clock.advance("14m");
	await setTimeout(200);
	expect(runs).toBe(1);

	await clock.advance("1m");
	await expect.poll(() => runs).toBe(2);

	await orchestrator.stop();
});

test("set keeps its own copy of the date", async () => {
	const date = new Date("2024-06-01T00:00:00Z");
	const setting = clock.set(date);
	date.setUTCFullYear(2030);
	await setting;
	date.setUTCFullYear(2031);

	const [row] = await sql<{ now: Date }[]>`
		select current_setting('pgconductor.fake_now')::timestamptz as now
	`;
	expect(row?.now).toEqual(new Date("2024-06-01T00:00:00Z"));
	expect(clock.now()).toEqual(new Date("2024-06-01T00:00:00Z"));
});
