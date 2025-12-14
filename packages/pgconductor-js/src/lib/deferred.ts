export class Deferred<T = void> {
	public promise: Promise<T>;
	public resolve!: (value: T | PromiseLike<T>) => void;
	public reject!: (reason?: unknown) => void;
	private _isSettled = false;

	constructor() {
		this.promise = new Promise<T>((res, rej) => {
			this.resolve = (value: T | PromiseLike<T>) => {
				this._isSettled = true;
				res(value);
			};
			this.reject = (reason?: unknown) => {
				this._isSettled = true;
				rej(reason);
			};
		});
	}

	get isSettled(): boolean {
		return this._isSettled;
	}
}
