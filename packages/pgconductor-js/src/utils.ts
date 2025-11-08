export interface AbortablePromise<T> extends Promise<T> {
	abort: () => void;
}

export function delay(ms: number, error?: string): AbortablePromise<void> {
	const ac = new AbortController();

	const promise = new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			if (error) {
				reject(new Error(error));
			} else {
				resolve();
			}
		}, ms);

		ac.signal.addEventListener("abort", () => {
			clearTimeout(timeout);
			resolve();
		});
	}) as AbortablePromise<void>;

	promise.abort = () => {
		if (!ac.signal.aborted) {
			ac.abort();
		}
	};

	return promise;
}

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
