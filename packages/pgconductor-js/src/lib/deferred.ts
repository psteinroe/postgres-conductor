export class Deferred<T = void> {
	public promise: Promise<T>;
	public resolve!: (value: T | PromiseLike<T>) => void;
	public reject!: (reason?: unknown) => void;

	constructor() {
		this.promise = new Promise<T>((res, rej) => {
			this.resolve = res;
			this.reject = rej;
		});
	}
}
