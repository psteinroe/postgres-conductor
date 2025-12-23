import { Conductor } from "../../src/conductor";
import { TaskSchemas } from "../../src/schemas";
import { TestDatabasePool, type TestDatabase } from "../../tests/fixtures/test-database";

export type BenchmarkContext = {
	conductor: Conductor<undefined, undefined, undefined, {}>;
	db: TestDatabase;
	pool: TestDatabasePool;
};

export class BenchmarkDatabase {
	static async create(): Promise<BenchmarkContext> {
		const pool = await TestDatabasePool.create();
		const db = await pool.child();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([]),
			context: {},
		});

		return { conductor, db, pool };
	}

	static async cleanup(ctx: BenchmarkContext): Promise<void> {
		await ctx.pool.destroy();
	}
}
