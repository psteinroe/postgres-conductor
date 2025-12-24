import { test } from "bun:test";
import { z } from "zod";
import type { SelectedRow } from "../../src/select-columns";

test("SelectedRow preserves case sensitivity", () => {
	// Test 1: Custom event payload (camelCase)
	type UserPayload = {
		userId: string;
		emailAddress: string;
		firstName: string;
		lastName: string;
	};

	type Selected = SelectedRow<UserPayload, "userId,emailAddress">;

	// Should work with correct case
	const test1: Selected = {
		userId: "123",
		emailAddress: "test@example.com",
	};

	// Extra field should cause error
	const test2: Selected = {
		userId: "123",
		emailAddress: "test@example.com",
		// @ts-expect-error - firstName not selected
		firstName: "John",
	};

	// Test 2: Database columns (snake_case)
	type ContactRow = {
		id: string;
		first_name: string;
		last_name: string | null;
		email: string | null;
	};

	type SelectedColumns = SelectedRow<ContactRow, "id,first_name,email">;

	// Should work with correct case
	const test3: SelectedColumns = {
		id: "1",
		first_name: "John",
		email: "john@example.com",
	};

	// Extra field should cause error
	const test4: SelectedColumns = {
		id: "1",
		first_name: "John",
		email: "john@example.com",
		// @ts-expect-error - last_name not selected
		last_name: "Doe",
	};

	// Test 3: Invalid field name should error at usage
	type InvalidSelection = SelectedRow<UserPayload, "UserId,emailAddress">;
	// @ts-expect-error - InvalidSelection should be an error type
	const test5: InvalidSelection = { UserId: "123", emailAddress: "test" };
});
