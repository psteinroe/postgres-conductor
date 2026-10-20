import type { StandardSchemaV1 } from "@standard-schema/spec";

export async function parseSchema<T>(
	schema: StandardSchemaV1<unknown, T> | undefined,
	value: T,
	label: string,
): Promise<{ value: T } | { error: string }> {
	if (!schema) return { value };
	const result = await schema["~standard"].validate(value);
	if (!result.issues) return { value: result.value };
	const issues = result.issues.map((issue) => {
		const path = issue.path
			?.map((segment) => String(typeof segment === "object" ? segment.key : segment))
			.join(".");
		return path ? `${path}: ${issue.message}` : issue.message;
	});
	return { error: `Invalid ${label}: ${issues.join("; ")}` };
}

export async function validateSchema<T>(
	schema: StandardSchemaV1<unknown, T> | undefined,
	value: T,
	label: string,
): Promise<T> {
	const parsed = await parseSchema(schema, value, label);
	if ("error" in parsed) throw new Error(parsed.error);
	return parsed.value;
}
