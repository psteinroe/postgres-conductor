export function createChildSignal(parentSignal?: AbortSignal): AbortController {
	const controller = new AbortController();

	if (parentSignal) {
		if (parentSignal.aborted) {
			controller.abort();
		} else {
			parentSignal.addEventListener("abort", () => {
				controller.abort();
			});
		}
	}

	return controller;
}
