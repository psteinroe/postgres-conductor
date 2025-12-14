export function coerceError(err: unknown): Error {
	if (err instanceof Error) {
		return err;
	} else {
		const message =
			typeof err === "object" && err !== null && "message" in err
				? String(err.message)
				: "An error occurred";
		return new Error(message, { cause: err });
	}
}
