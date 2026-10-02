export function uuidv7(): string {
	const time = Date.now().toString(16).padStart(12, "0");
	const random = crypto.randomUUID();
	return `${time.slice(0, 8)}-${time.slice(8)}-7${random.slice(15, 18)}-${random.slice(19)}`;
}
