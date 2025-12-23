import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { TestDatabasePool, type TestDatabase } from "../../tests/fixtures/test-database";

const taskDefinitions = [
	defineTask({ name: "simple" }),
	defineTask({ name: "with-steps" }),
	defineTask({ name: "work" }),
	defineTask({ name: "work-q1", queue: "q1" }),
	defineTask({ name: "work-q2", queue: "q2" }),
	defineTask({ name: "work-q3", queue: "q3" }),
	defineTask({ name: "work-q4", queue: "q4" }),
	defineTask({ name: "unlimited" }),
	defineTask({ name: "limited" }),
] as const;

export class BenchmarkDatabase {
	static async create() {
		const pool = await TestDatabasePool.create();
		const db = await pool.child();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(taskDefinitions),
			context: {},
		});

		return { conductor, db, pool };
	}

	static async cleanup(ctx: BenchmarkContext): Promise<void> {
		await ctx.pool.destroy();
	}
}

export type BenchmarkContext = Awaited<ReturnType<typeof BenchmarkDatabase.create>>;
