import { startPostgres } from "../src/testing";

// Test files run in worker processes started after this, so they inherit DATABASE_URL
export default async function setup() {
	const postgres = await startPostgres({ image: "postgres:15" });
	process.env.DATABASE_URL = postgres.url;
	return postgres.stop;
}
