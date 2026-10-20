import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, beforeAll, expect, test } from "vitest";
import { TestDatabasePool } from "../fixtures/test-database";

const run = promisify(execFile);

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

test("generates types for tables named like Object.prototype properties", async () => {
	const db = await pool.child();
	await db.sql.unsafe(`
		create table public."constructor" (id int4 not null);
		create table public."__proto__" (name text);
	`);

	const { stdout } = await run("node", [
		"--import",
		"tsx",
		"cli/index.ts",
		"gen",
		"types",
		"typescript",
		"--db-url",
		db.url,
	]);

	expect(stdout).toContain(`    "__proto__": {\n      "name": string | null;\n    };`);
	expect(stdout).toContain(`    "constructor": {\n      "id": number;\n    };`);
});
