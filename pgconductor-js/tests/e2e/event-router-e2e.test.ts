import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import postgres from "postgres";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { Network } from "testcontainers";
import * as path from "path";

describe("Event Router E2E", () => {
	let network: any;
	let pgContainer: StartedTestContainer;
	let eventRouterContainer: StartedTestContainer;
	let sql: ReturnType<typeof postgres>;
	let masterUrl: string;

	beforeAll(async () => {
		// Create a shared network
		network = await new Network().start();

		// Start PostgreSQL with logical replication enabled
		pgContainer = await new GenericContainer("postgres:15")
			.withNetwork(network)
			.withNetworkAliases("postgres")
			.withEnvironment({
				POSTGRES_PASSWORD: "postgres",
				POSTGRES_USER: "postgres",
				POSTGRES_DB: "postgres",
			})
			.withCommand([
				"postgres",
				"-c",
				"wal_level=logical",
				"-c",
				"max_replication_slots=10",
				"-c",
				"max_wal_senders=10",
			])
			.withExposedPorts(5432)
			.withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
			.start();

		const host = pgContainer.getHost();
		const port = pgContainer.getMappedPort(5432);

		// Connect to default database to create test database
		const adminSql = postgres(`postgres://postgres:postgres@${host}:${port}/postgres`, { max: 1 });
		await adminSql.unsafe(`CREATE DATABASE pgconductor_test`);
		await adminSql.end();

		// Connect to the new test database
		masterUrl = `postgres://postgres:postgres@${host}:${port}/pgconductor_test`;
		sql = postgres(masterUrl, { max: 1 });

		// Install uuid-ossp extension (needed by migrations)
		await sql`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`;

		// Run a setup orchestrator to apply migrations
		const setupTask = defineTask({
			name: "setup-task",
			payload: z.object({}),
		});

		const setupConductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([setupTask]),
			context: {},
		});

		const setupOrchestrator = Orchestrator.create({
			conductor: setupConductor,
			tasks: [
				setupConductor.createTask({ name: "setup-task" }, { invocable: true }, async () => {}),
			],
		});

		await setupOrchestrator.start();
		await setupOrchestrator.stop();

		// Create application tables for testing database events
		await sql.unsafe(`
			CREATE TABLE address_book (
				id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
				name text NOT NULL,
				description text,
				created_at timestamptz NOT NULL DEFAULT now(),
				updated_at timestamptz NOT NULL DEFAULT now()
			);

			CREATE TABLE contact (
				id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
				address_book_id uuid NOT NULL REFERENCES address_book(id),
				first_name text NOT NULL,
				last_name text,
				email text,
				phone text,
				is_favorite boolean NOT NULL DEFAULT false,
				metadata jsonb,
				created_at timestamptz NOT NULL DEFAULT now()
			);
		`);

		// Create publication for CDC - only need events and subscriptions tables
		await sql.unsafe(`
			CREATE PUBLICATION pgconductor_events
			FOR TABLE pgconductor.events, pgconductor.subscriptions
		`);

		// Build and start event-router container from Dockerfile
		// Note: Using debug build for faster compilation during tests
		// To force rebuild: docker rmi pgconductor-event-router:test-debug
		const projectRoot = path.resolve(__dirname, "../../../..");
		const imageName = "pgconductor-event-router:test-debug";

		// Check if image already exists
		const { execSync } = await import("child_process");
		let imageExists = false;
		try {
			execSync(`docker image inspect ${imageName}`, { stdio: "ignore" });
			imageExists = true;
		} catch {
			imageExists = false;
		}

		let eventRouterImage;
		if (imageExists) {
			eventRouterImage = new GenericContainer(imageName);
		} else {
			eventRouterImage = await GenericContainer.fromDockerfile(
				projectRoot,
				"crates/event-router/Dockerfile.debug",
			)
				.withCache(true)
				.build(imageName);
		}

		eventRouterContainer = await eventRouterImage
			.withNetwork(network)
			.withEnvironment({
				PGHOST: "postgres",
				PGPORT: "5432",
				PGDATABASE: "pgconductor_test",
				PGUSER: "postgres",
				PGPASSWORD: "postgres",
				PUBLICATION_NAME: "pgconductor_events",
				RUST_LOG: "debug,event_router=debug",
			})
			.withWaitStrategy(Wait.forLogMessage(/Subscriptions loaded, starting pipeline/))
			.start();

		// Give the event-router time to establish CDC connection
		await new Promise((r) => setTimeout(r, 2000));
	}, 600000); // 10 minutes - Rust build takes time

	afterAll(async () => {
		await sql?.end();
		await eventRouterContainer?.stop();
		await pgContainer?.stop();
		await network?.stop();
	});

	test("event-router wakes execution when event is emitted", async () => {
		const taskDefinition = defineTask({
			name: "e2e-waiter",
			payload: z.object({}),
			returns: z.object({ eventData: z.any() }),
		});

		let receivedData: any = null;

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const waiterTask = conductor.createTask(
			{ name: "e2e-waiter" },
			{ invocable: true },
			async (_event, ctx) => {
				const eventData = await ctx.waitForEvent("wait-for-user", {
					event: "user.created",
				});
				receivedData = eventData;
				return { eventData };
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [waiterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Invoke the task that will wait for an event
		await conductor.invoke({ name: "e2e-waiter" }, {});

		// Wait for task to create subscription
		await new Promise((r) => setTimeout(r, 1000));

		// Verify subscription was created
		const subscriptions = await sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE event_key = 'user.created'
		`;
		expect(subscriptions.length).toBe(1);

		// Emit the event - this will be picked up by event-router via CDC
		await sql`
			INSERT INTO pgconductor.events (event_key, payload)
			VALUES ('user.created', ${sql.json({ userId: 456 })})
		`;

		// Wait for event-router to process the event and wake the execution
		// This is the actual E2E test - the event-router should:
		// 1. Detect the new event via CDC
		// 2. Match it to the subscription
		// 3. Call wake_execution to save the step and wake the task
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task received the event data
		expect(receivedData).toEqual({ userId: 456 });

		// Verify execution completed
		const executions = await sql`
			SELECT completed_at FROM pgconductor.executions
			WHERE task_key = 'e2e-waiter'
		`;
		expect(executions[0]?.completed_at).not.toBeNull();
	}, 30000);

	test("event-router handles multiple subscriptions", async () => {
		const taskDefinition = defineTask({
			name: "multi-waiter",
			payload: z.object({ eventKey: z.string() }),
			returns: z.object({ received: z.boolean() }),
		});

		const receivedEvents: string[] = [];

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const multiWaiterTask = conductor.createTask(
			{ name: "multi-waiter" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					const payload = event.payload as { eventKey: string };
					await ctx.waitForEvent("wait-step", { event: payload.eventKey });
					receivedEvents.push(payload.eventKey);
					return { received: true };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [multiWaiterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Invoke multiple tasks waiting for different events
		await conductor.invoke({ name: "multi-waiter" }, { eventKey: "order.created" });
		await conductor.invoke({ name: "multi-waiter" }, { eventKey: "payment.completed" });

		// Wait for subscriptions to be created
		await new Promise((r) => setTimeout(r, 1000));

		// Emit both events
		await sql`
			INSERT INTO pgconductor.events (event_key, payload)
			VALUES ('order.created', ${sql.json({ orderId: 1 })})
		`;
		await sql`
			INSERT INTO pgconductor.events (event_key, payload)
			VALUES ('payment.completed', ${sql.json({ paymentId: 2 })})
		`;

		// Wait for event-router to process
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Both events should have been received
		expect(receivedEvents).toContain("order.created");
		expect(receivedEvents).toContain("payment.completed");
	}, 30000);

	test("event-router handles database table CDC events", async () => {
		const taskDefinition = defineTask({
			name: "contact-watcher",
			payload: z.object({}),
			returns: z.object({ contactId: z.string().optional() }),
		});

		let receivedContactId: string | undefined = undefined;

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const contactWatcherTask = conductor.createTask(
			{ name: "contact-watcher" },
			{ invocable: true },
			async (_event, ctx) => {
				// Wait for a contact to be created via database CDC
				// Payload is trigger-like: { old, new, tg_table, tg_op }
				const data = await ctx.waitForEvent("wait-for-contact", {
					schema: "public",
					table: "contact",
					operation: "insert",
				});
				// new is a JSON object with column names as keys
				const newData = data.new as { id: string };
				receivedContactId = newData.id;
				return { contactId: newData.id };
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [contactWatcherTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Invoke the task that will wait for a contact creation
		await conductor.invoke({ name: "contact-watcher" }, {});

		// Wait for task to create subscription
		await new Promise((r) => setTimeout(r, 1000));

		// Create an address book first (required by foreign key)
		const [addressBook] = await sql`
			INSERT INTO address_book (name, description)
			VALUES ('Test Address Book', 'For E2E testing')
			RETURNING id
		`;

		// Insert a contact - this will be picked up by CDC automatically
		const [contact] = await sql`
			INSERT INTO contact (address_book_id, first_name, last_name, email)
			VALUES (${addressBook!.id}, 'John', 'Doe', 'john@example.com')
			RETURNING id
		`;

		// Wait for event-router to process the CDC event
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task received the contact data
		expect(receivedContactId).toBe(contact!.id);
	}, 30000);

	test("event-router filters columns in database CDC events", async () => {
		const taskDefinition = defineTask({
			name: "column-filter-watcher",
			payload: z.object({}),
			returns: z.object({ data: z.any() }),
		});

		let receivedData: any = null;

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const columnWatcherTask = conductor.createTask(
			{ name: "column-filter-watcher" },
			{ invocable: true },
			async (_event, ctx) => {
				// Wait for a contact to be created, but only receive id and email columns
				// Note: Using type assertion since e2e creates tables dynamically
				const data = await ctx.waitForEvent("wait-for-contact-columns", {
					schema: "public",
					table: "contact",
					operation: "insert",
					columns: "id, email" as any,
				});
				receivedData = data;
				return { data };
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [columnWatcherTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Invoke the task that will wait for a contact creation with column filter
		await conductor.invoke({ name: "column-filter-watcher" }, {});

		// Wait for task to create subscription
		await new Promise((r) => setTimeout(r, 1000));

		// Verify subscription was created with columns
		const subscriptions = await sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE step_key = 'wait-for-contact-columns'
		`;
		expect(subscriptions.length).toBe(1);
		expect(subscriptions[0]?.columns).toEqual(["id", "email"]);

		// Create an address book first (required by foreign key)
		const [addressBook] = await sql`
			INSERT INTO address_book (name, description)
			VALUES ('Column Filter Test', 'For column filtering E2E testing')
			RETURNING id
		`;

		// Insert a contact with many fields
		const [contact] = await sql`
			INSERT INTO contact (address_book_id, first_name, last_name, email, phone)
			VALUES (${addressBook!.id}, 'Jane', 'Smith', 'jane@example.com', '555-1234')
			RETURNING id
		`;

		// Wait for event-router to process the CDC event
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task received only the selected columns
		expect(receivedData).toBeDefined();
		expect(receivedData.tg_op).toBe("INSERT");
		expect(receivedData.old).toBeNull();

		// The new object should only have id and email, not first_name, last_name, phone, etc.
		const newData = receivedData.new;
		expect(newData.id).toBe(contact!.id);
		expect(newData.email).toBe("jane@example.com");

		// These should NOT be present due to column filtering
		expect(newData.first_name).toBeUndefined();
		expect(newData.last_name).toBeUndefined();
		expect(newData.phone).toBeUndefined();
	}, 30000);

	test("event-router invokes task from custom event trigger", async () => {
		// Define the event
		const userCreated = defineEvent({
			name: "user.registered",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const taskDefinition = defineTask({
			name: "on-user-registered",
		});

		const receivedPayloads: any[] = [];

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		// Create task with event trigger (not invocable)
		const onUserRegisteredTask = conductor.createTask(
			{ name: "on-user-registered" },
			{ event: "user.registered" },
			async (event, _ctx) => {
				if (event.event === "user.registered") {
					receivedPayloads.push(event.payload);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [onUserRegisteredTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Give time for worker to register subscriptions
		await new Promise((r) => setTimeout(r, 1000));

		// Verify subscription was created
		const subscriptions = await sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE event_key = 'user.registered' AND task_key = 'on-user-registered'
		`;
		expect(subscriptions.length).toBe(1);
		expect(subscriptions[0]?.execution_id).toBeNull(); // Trigger-based has no execution_id

		// Emit the event - this should invoke the task
		await sql`
			INSERT INTO pgconductor.events (event_key, payload)
			VALUES ('user.registered', ${sql.json({ userId: "123", email: "test@example.com" })})
		`;

		// Wait for event-router to process and task to execute
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task was invoked with the event payload
		expect(receivedPayloads.length).toBe(1);
		expect(receivedPayloads[0]).toEqual({
			userId: "123",
			email: "test@example.com",
		});

		// Verify execution was created and completed
		const executions = await sql`
			SELECT * FROM pgconductor.executions
			WHERE task_key = 'on-user-registered'
		`;
		expect(executions.length).toBe(1);
		expect(executions[0]?.completed_at).not.toBeNull();
	}, 30000);

	test("event-router invokes task from database event trigger", async () => {
		const taskDefinition = defineTask({
			name: "on-contact-created",
		});

		const receivedPayloads: any[] = [];

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		// Create task with database event trigger
		const onContactCreatedTask = conductor.createTask(
			{ name: "on-contact-created" },
			{ schema: "public", table: "contact", operation: "insert" },
			async (event, _ctx) => {
				// Event payload has the structure: { event, payload: { old, new, tg_table, tg_op } }
				receivedPayloads.push(event);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [onContactCreatedTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Give time for worker to register subscriptions
		await new Promise((r) => setTimeout(r, 1000));

		// Verify subscription was created
		const subscriptions = await sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE task_key = 'on-contact-created' AND source = 'db'
		`;
		expect(subscriptions.length).toBe(1);
		expect(subscriptions[0]?.execution_id).toBeNull(); // Trigger-based

		// Create an address book first
		const [addressBook] = await sql`
			INSERT INTO address_book (name, description)
			VALUES ('Trigger Test', 'For trigger-based E2E testing')
			RETURNING id
		`;

		// Insert a contact - this should trigger the task
		await sql`
			INSERT INTO contact (address_book_id, first_name, last_name, email)
			VALUES (${addressBook!.id}, 'Trigger', 'Test', 'trigger@example.com')
			RETURNING id
		`;

		// Wait for event-router to process and task to execute
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task was invoked
		expect(receivedPayloads.length).toBe(1);

		// Verify execution was created and completed
		const executions = await sql`
			SELECT * FROM pgconductor.executions
			WHERE task_key = 'on-contact-created'
		`;
		expect(executions.length).toBe(1);
		expect(executions[0]?.completed_at).not.toBeNull();
	}, 30000);

	test("event-router invokes task from database event trigger with column selection", async () => {
		const taskDefinition = defineTask({
			name: "on-contact-columns-trigger",
		});

		let receivedPayload: any = null;

		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		// Create task with database event trigger and column selection
		const onContactColumnsTask = conductor.createTask(
			{ name: "on-contact-columns-trigger" },
			{ schema: "public", table: "contact", operation: "insert", columns: "id, email" },
			async (event, _ctx) => {
				receivedPayload = event.payload;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [onContactColumnsTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Give time for worker to register subscriptions
		await new Promise((r) => setTimeout(r, 1000));

		// Verify subscription was created with columns
		const subscriptions = await sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE task_key = 'on-contact-columns-trigger' AND source = 'db'
		`;
		expect(subscriptions.length).toBe(1);
		expect(subscriptions[0]?.columns).toEqual(["id", "email"]);

		// Create an address book first
		const [addressBook] = await sql`
			INSERT INTO address_book (name, description)
			VALUES ('Column Filter Trigger Test', 'For column filtering trigger E2E testing')
			RETURNING id
		`;

		// Insert a contact with many fields
		const [contact] = await sql`
			INSERT INTO contact (address_book_id, first_name, last_name, email, phone)
			VALUES (${addressBook!.id}, 'Column', 'Trigger', 'column-trigger@example.com', '555-9999')
			RETURNING id
		`;

		// Wait for event-router to process and task to execute
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Verify the task received only the selected columns
		expect(receivedPayload).toBeDefined();
		expect(receivedPayload.tg_op).toBe("INSERT");
		expect(receivedPayload.old).toBeNull();

		// The new object should only have id and email
		const newData = receivedPayload.new;
		expect(newData.id).toBe(contact!.id);
		expect(newData.email).toBe("column-trigger@example.com");

		// These should NOT be present due to column filtering
		expect(newData.first_name).toBeUndefined();
		expect(newData.last_name).toBeUndefined();
		expect(newData.phone).toBeUndefined();

		// Verify execution was created and completed
		const executions = await sql`
			SELECT * FROM pgconductor.executions
			WHERE task_key = 'on-contact-columns-trigger'
		`;
		expect(executions.length).toBe(1);
		expect(executions[0]?.completed_at).not.toBeNull();
	}, 30000);
});
