type Digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";

type LowerLetter =
	| "a"
	| "b"
	| "c"
	| "d"
	| "e"
	| "f"
	| "g"
	| "h"
	| "i"
	| "j"
	| "k"
	| "l"
	| "m"
	| "n"
	| "o"
	| "p"
	| "q"
	| "r"
	| "s"
	| "t"
	| "u"
	| "v"
	| "w"
	| "x"
	| "y"
	| "z";

type UpperLetter =
	| "A"
	| "B"
	| "C"
	| "D"
	| "E"
	| "F"
	| "G"
	| "H"
	| "I"
	| "J"
	| "K"
	| "L"
	| "M"
	| "N"
	| "O"
	| "P"
	| "Q"
	| "R"
	| "S"
	| "T"
	| "U"
	| "V"
	| "W"
	| "X"
	| "Y"
	| "Z";

type Letter = LowerLetter | UpperLetter;

type FirstChar = Letter | "_";
type RestChar = Letter | Digit | "_";

// recursive check for rest of unquoted identifier
type AllRest<S extends string> = S extends ""
	? true
	: S extends `${infer C}${infer R}`
		? C extends RestChar
			? AllRest<R>
			: false
		: true;

// [a-zA-Z_][a-zA-Z0-9_]*
type IsValidUnquoted<S extends string> = S extends `${infer F}${infer R}`
	? F extends FirstChar
		? AllRest<R>
		: false
	: false;

type Trim<S extends string> = S extends ` ${infer R}`
	? Trim<R>
	: S extends `${infer R} `
		? Trim<R>
		: S;

type Split<S extends string> = S extends `${infer Head},${infer Rest}`
	? [Head, ...Split<Rest>]
	: [S];

export type ColumnSelectionError<Message extends string> = {
	readonly __columnSelectionError: Message;
};

type IsError<T> = T extends `[Column Selection Error]: ${string}` ? true : false;

type HasError<T extends readonly any[]> = true extends IsError<T[number]> ? true : false;

type ExtractKeys<T extends readonly any[]> = T[number] extends infer U
	? U extends string
		? IsError<U> extends true
			? never
			: U
		: never
	: never;

type FirstError<T extends readonly any[]> = T extends [infer Head, ...infer Tail]
	? Head extends string
		? IsError<Head> extends true
			? Head
			: Tail extends readonly any[]
				? FirstError<Tail>
				: undefined
		: Tail extends readonly any[]
			? FirstError<Tail>
			: undefined
	: undefined;

type PickFromKeys<Keys extends keyof Obj, Obj> = { [K in Keys]: Obj[K] };

type ValidatePart<Part extends string, Obj> =
	Trim<Part> extends infer P extends string
		? P extends ""
			? `[Column Selection Error]: Empty entry. Remove trailing commas or extra spaces.`
			: // quoted identifier
				P extends `"${infer Body}"`
				? // forbid raw " inside quoted body
					Body extends `${string}"${string}`
					? `[Column Selection Error]: Invalid quote in "${P}". Use "" to escape quotes inside identifiers.`
					: Body extends keyof Obj
						? Body
						: `[Column Selection Error]: Column "${Body}" does not exist.`
				: // malformed quoting
					P extends `${string}"${string}`
					? `[Column Selection Error]: Malformed quotes in "${P}". Quoted identifiers must be fully wrapped.`
					: // unquoted identifier
						IsValidUnquoted<P> extends false
						? `[Column Selection Error]: Invalid identifier "${P}". Must start with a letter or underscore.`
						: P extends keyof Obj
							? P
							: `[Column Selection Error]: Column "${P}" does not exist.`
		: never;

type ValidateParts<Parts extends readonly string[], Obj> = {
	[I in keyof Parts]: ValidatePart<Parts[I], Obj>;
};

export type ParseSelection<S extends string, Obj> =
	Split<S> extends infer Parts extends string[]
		? ValidateParts<Parts, Obj> extends infer V extends any[]
			? HasError<V> extends true
				? FirstError<V> extends string
					? ColumnSelectionError<FirstError<V>>
					: never
				: PickFromKeys<ExtractKeys<V>, Obj>
			: never
		: never;

export type SelectedRow<Row, S extends string> = ParseSelection<S, Row>;

// Validate columns string - returns the string if valid, or error string if invalid
export type ValidateColumns<S extends string, Row> =
	ParseSelection<S, Row> extends ColumnSelectionError<infer Message> ? Message : S;
