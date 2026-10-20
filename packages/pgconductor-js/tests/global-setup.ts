import type { TestProject } from "vitest/node";
import { startPostgres } from "./fixtures/test-database";

export default async function setup(project: TestProject) {
	const postgres = await startPostgres();
	project.provide("databaseUrl", postgres.url);
	return postgres.stop;
}

declare module "vitest" {
	export interface ProvidedContext {
		databaseUrl: string;
	}
}
