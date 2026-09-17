export async function waitForCondition(
	condition: () => boolean | Promise<boolean>,
	timeoutMs = 20_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!(await condition())) {
		if (Date.now() >= deadline) {
			throw new Error(`condition was not met within ${timeoutMs}ms`);
		}
		await Bun.sleep(25);
	}
}
