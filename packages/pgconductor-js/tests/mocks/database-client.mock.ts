import { mock } from "bun:test";
import type { DatabaseClient } from "../../src/database-client";

type PublicMethodsOf<T> = {
	[K in keyof T as T[K] extends Function ? K : never]: T[K];
};

type IDatabaseClient = PublicMethodsOf<DatabaseClient>;

export class MockDatabaseClient implements IDatabaseClient {
	close = mock(async () => {});
	orchestratorHeartbeat = mock(async () => false);
	recoverStaleOrchestrators = mock(async () => {});
	sweepOrchestrators = mock(async () => {});
	getInstalledMigrationNumber = mock(async () => -1);
	applyMigration = mock(async () => "applied" as const);
	countActiveOrchestratorsBelow = mock(async () => 0);
	orchestratorShutdown = mock(async () => {});
	getExecutions = mock(async () => []);
	returnExecutions = mock(async () => {});
	upsertTask = mock(async () => {});
	invoke = mock(async () => {});
	invokeBatch = mock(async () => {});
	loadStep = mock(async () => null);
	saveStep = mock(async () => {});
	clearWaitingState = mock(async () => {});

	constructor(overrides: Partial<IDatabaseClient> = {}) {
		Object.assign(this, overrides);
	}
}
