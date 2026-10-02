import type { StandardSchemaV1 } from "@standard-schema/spec";

export async function validateSchema<T>(
	schema: StandardSchemaV1<unknown, T> | undefined,
	value: T,
	label: string,
): Promise<T> {
	if (!schema) return value;
	const result = await schema["~standard"].validate(value);
	if (result.issues) {
		const issues = result.issues.map((issue) => {
			const path = issue.path
				?.map((segment) => String(typeof segment === "object" ? segment.key : segment))
				.join(".");
			return path ? `${path}: ${issue.message}` : issue.message;
		});
		throw new Error(`Invalid ${label}: ${issues.join("; ")}`);
	}
	return result.value;
}
