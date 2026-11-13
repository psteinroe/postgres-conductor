import { vi } from "vitest";
import type { DatabaseClient } from "../../src/database-client";

type PublicMethodsOf<T> = {
	[K in keyof T as T[K] extends Function ? K : never]: T[K];
};

type IDatabaseClient = PublicMethodsOf<DatabaseClient>;

export class MockDatabaseClient implements IDatabaseClient {
	close = vi.fn(async () => {});
	withOwnConnection = vi.fn((): DatabaseClient => {
		throw new Error("MockDatabaseClient has no connection");
	});
	registerOrchestrator = vi.fn(async () => ({ shutdown_reason: null }));
	orchestratorHeartbeat = vi.fn(async () => ({ shutdown_reason: null }));
	recoverStaleOrchestrators = vi.fn(async () => {});
	sweepOrchestrators = vi.fn(async () => {});
	getInstalledMigrationNumber = vi.fn(async () => -1);
	applyMigration = vi.fn(async () => "applied" as const);
	countActiveOrchestratorsBelow = vi.fn(async () => 0);
	orchestratorShutdown = vi.fn(async () => {});
	getExecution = vi.fn(async () => null);
	listExecutions = vi.fn(async () => []);
	getEvent = vi.fn(async () => null);
	listEvents = vi.fn(async () => []);
	countEvents = vi.fn(async () => 0);
	getExecutions = vi.fn(async () => ({ executions: [], signals: [], cancelled: [] }));
	returnExecutions = vi.fn(async (_results) => {});
	removeExecutions = vi.fn(async () => false);
	registerWorker = vi.fn(async () => {});
	scheduleCronExecution = vi.fn(async () => {});
	unscheduleCronExecution = vi.fn(async () => {});
	scheduleNextCronExecution = vi.fn(async () => {});
	invoke = vi.fn(async () => "mock-id");
	invokeChild = vi.fn(async () => "mock-child-id");
	invokeBatch = vi.fn(async () => ["mock-id"]);
	loadStep = vi.fn(async () => null);
	countSteps = vi.fn(async () => 0);
	saveStep = vi.fn(async (_args) => true);
	setState = vi.fn(async (_args) => true);
	startExecution = vi.fn(async () => ({ id: "mock-started-id", started: true }));
	checkMetadata = vi.fn(async () => {});
	clearWaitingState = vi.fn(async () => true);
	registerEventWait = vi.fn(async () => null);
	registerEventRace = vi.fn(async () => null);
	cancelExecution = vi.fn(async () => true);
	ackSignals = vi.fn(async () => {});
	signalExecution = vi.fn(async () => true);
	signalWithStart = vi.fn(async () => ({ id: "mock-execution-id", started: true }));
	signalWithStartStep = vi.fn(async () => ({ id: "mock-execution-id", started: true }));
	getDatabaseTime = vi.fn(async () => new Date());
	setFakeTime = vi.fn(async () => {});
	clearFakeTime = vi.fn(async () => {});
	emitEvent = vi.fn(async () => "mock-event-id");
	dispatchCustomEvents = vi.fn(async ({ eventIds }: { eventIds: string[] }) => eventIds);
	setListens = vi.fn(async (): Promise<string[] | null> => []);
	listListeners = vi.fn(async () => []);

	constructor(overrides: Partial<IDatabaseClient> = {}) {
		Object.assign(this, overrides);
	}
}
