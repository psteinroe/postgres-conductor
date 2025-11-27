#!/usr/bin/env node

import postgres from "postgres";
import prettier from "prettier";
import * as assert from "./lib/assert";

interface Column {
	table_schema: string;
	table_name: string;
	column_name: string;
	data_type: string;
	udt_name: string;
	is_nullable: string;
}

function pgTypeToTsType(udtName: string): string {
	if (udtName === "bool") {
		return "boolean";
	} else if (["int2", "int4", "int8", "float4", "float8", "numeric"].includes(udtName)) {
		return "number";
	} else if (
		[
			"bytea",
			"bpchar",
			"varchar",
			"date",
			"text",
			"citext",
			"time",
			"timetz",
			"timestamp",
			"timestamptz",
			"uuid",
			"vector",
		].includes(udtName)
	) {
		return "string";
	} else if (["json", "jsonb"].includes(udtName)) {
		return "Json";
	} else if (udtName === "void") {
		return "undefined";
	} else if (udtName.startsWith("_")) {
		return `(${pgTypeToTsType(udtName.substring(1))})[]`;
	}
	return "unknown";
}

async function generateTypes(dbUrl: string, schemas: string[]): Promise<string> {
	const sql = postgres(dbUrl);

	try {
		const columns = await sql<Column[]>`
      select
        table_schema,
        table_name,
        column_name,
        data_type,
        udt_name,
        is_nullable
      from information_schema.columns
      where table_schema = any(${schemas})
        and table_name in (
          select table_name
          from information_schema.tables
          where table_schema = any(${schemas})
            and table_type = 'BASE TABLE'
        )
      order by table_schema, table_name, ordinal_position
    `;

		// Group columns by schema and table
		const schemaMap: Record<string, Record<string, Column[]>> = {};

		for (const col of columns) {
			if (!schemaMap[col.table_schema]) {
				schemaMap[col.table_schema] = {};
			}
			const schemaEntry = schemaMap[col.table_schema];
			assert.ok(schemaEntry, `Schema entry for ${col.table_schema} should exist`);
			if (!schemaEntry[col.table_name]) {
				schemaEntry[col.table_name] = [];
			}
			const tableEntry = schemaEntry[col.table_name];
			assert.ok(tableEntry, `Table entry for ${col.table_name} should exist`);
			tableEntry.push(col);
		}

		// Generate output
		let output = `export type Json = string | number | boolean | null | { [key: string]: Json | undefined } | Json[]

export type Database = {
`;

		for (const schema of schemas) {
			const tables = schemaMap[schema] || {};
			output += `  ${JSON.stringify(schema)}: {\n`;

			const tableNames = Object.keys(tables).sort();
			for (const tableName of tableNames) {
				const cols = tables[tableName] || [];
				output += `    ${JSON.stringify(tableName)}: {\n`;

				for (const col of cols) {
					const tsType = pgTypeToTsType(col.udt_name);
					const nullable = col.is_nullable === "YES";
					const finalType = nullable ? `${tsType} | null` : tsType;
					output += `      ${JSON.stringify(col.column_name)}: ${finalType};\n`;
				}

				output += `    };\n`;
			}

			output += `  };\n`;
		}

		output += `}\n`;

		// Format with prettier
		output = await prettier.format(output, {
			parser: "typescript",
			semi: true,
		});

		return output;
	} finally {
		await sql.end();
	}
}

function printUsage() {
	console.error(`Usage: pgconductor gen types typescript [options]

Options:
  --db-url <url>      PostgreSQL connection URL (default: postgres://postgres:postgres@localhost:5432/postgres)
  --schemas <schemas> Comma-separated list of schemas (default: public)

Example:
  pgconductor gen types typescript
  pgconductor gen types typescript --db-url "postgres://user:pass@localhost/db" --schemas "public,myschema"
`);
}

function parseArgs(args: string[]): { dbUrl: string; schemas: string[] } {
	let dbUrl = "postgres://postgres:postgres@localhost:5432/postgres";
	let schemasArg = "public";

	for (let i = 0; i < args.length; i++) {
		const nextArg = args[i + 1];
		if (args[i] === "--db-url" && nextArg) {
			dbUrl = nextArg;
			i++;
		} else if (args[i] === "--schemas" && nextArg) {
			schemasArg = nextArg;
			i++;
		}
	}

	return {
		dbUrl,
		schemas: schemasArg.split(",").map((s) => s.trim()),
	};
}

async function main() {
	const args = process.argv.slice(2);

	if (args.length < 3) {
		printUsage();
		process.exit(1);
	}

	const [cmd1, cmd2, cmd3, ...rest] = args;

	if (cmd1 !== "gen" || cmd2 !== "types" || cmd3 !== "typescript") {
		printUsage();
		process.exit(1);
	}

	const { dbUrl, schemas } = parseArgs(rest);

	try {
		const output = await generateTypes(dbUrl, schemas);
		console.log(output);
	} catch (error) {
		console.error("Error generating types:", error);
		process.exit(1);
	}
}

main().catch((e) => console.error(e));
