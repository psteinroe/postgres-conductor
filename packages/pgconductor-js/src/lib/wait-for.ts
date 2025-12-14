/**
 * Delay that respects an AbortSignal with optional jitter.
 * Resolves after ms (+ jitter), or immediately if signal is aborted.
 */
export function waitFor(
	ms: number,
	options?: { signal?: AbortSignal; jitter?: number },
): Promise<void> {
	return new Promise<void>((resolve) => {
		if (options?.signal?.aborted) {
			resolve();
			return;
		}

		const jitter = options?.jitter || 0;
		const delay = jitter > 0 ? ms + Math.random() * jitter : ms;
		const timeout = setTimeout(resolve, delay);

		if (options?.signal) {
			options.signal.addEventListener(
				"abort",
				() => {
					clearTimeout(timeout);
					resolve();
				},
				{
					once: true,
				},
			);
		}
	});
}
