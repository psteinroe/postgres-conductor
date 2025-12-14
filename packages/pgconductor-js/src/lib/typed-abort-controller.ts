export class TypedAbortController<T = unknown> extends AbortController {
	override abort(reason: T): void {
		super.abort(reason);
	}

	override get signal(): AbortSignal & { reason: T | undefined } {
		return super.signal as any;
	}
}
