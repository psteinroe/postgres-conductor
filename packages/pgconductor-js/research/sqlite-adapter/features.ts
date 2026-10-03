import { Database } from "bun:sqlite";

// Tiny executable probes: failures are retained as evidence, not inferred from docs.
const db = new Database(":memory:");
const results: { feature: string; supported: boolean; rows?: unknown; error?: string }[] = [];
function probe(feature: string, sql: string) {
	try {
		results.push({ feature, supported: true, rows: db.query(sql).all() });
	} catch (error) {
		results.push({
			feature,
			supported: false,
			error: error instanceof Error ? error.message : String(error),
		});
	}
}
db.transaction(() => {
	db.exec(`create table t (id integer primary key, task_key text, singleton_on integer, dedupe_key text,
		queue text, completed_at integer, failed_at integer, cancelled integer default 0, value integer);
		create unique index singleton on t (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
		where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false;
		insert into t (id, task_key, singleton_on, queue, value) values (1, 'task', 10, 'default', 1);`);
	probe("SQLite version", "select sqlite_version() as version");
	probe("DML inside CTE", "with x as (update t set value = 2 returning *) select * from x");
	probe(
		"partial expression index conflict target",
		`insert into t (id, task_key, singleton_on, queue) values (2, 'task', 10, 'default')
		on conflict (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
		where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false do nothing returning id`,
	);
	probe(
		"UPDATE FROM",
		"update t set value = source.value from (select 7 as value) source where t.id = 1 returning value",
	);
	probe(
		"RETURNING with UPSERT",
		"insert into t (id, value) values (1, 9) on conflict (id) do update set value = excluded.value returning id, value",
	);
	probe(
		"RETURNING inside trigger",
		"create trigger test after insert on t begin update t set value = 0 where id = new.id returning id; end",
	);
	probe(
		"INSERT RETURNING inside trigger",
		"create trigger test_insert after insert on t begin insert into t (id) values (new.id + 100) returning id; end",
	);
	probe(
		"json_type scalar types",
		`select json_type('1') as integer_number, json_type('1.5') as real_number,
		json_type('true') as boolean_true, json_type('false') as boolean_false,
		json_type('null') as json_null, json_type('{}') as object, json_type('[]') as array, json_type('"hi"') as string`,
	);
}).immediate();
db.close();
console.log(JSON.stringify({ bun: Bun.version, results }, null, 2));
