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

type HasError<T extends readonly any[]> = T[number] extends { error: string }
	? true
	: false;

type ExtractKeys<T extends readonly any[]> = Extract<T[number], string>;

type PickFromKeys<Keys extends keyof Obj, Obj> = { [K in Keys]: Obj[K] };

type ValidatePart<Part extends string, Obj> = Trim<Part> extends infer P extends
	string
	? P extends ""
		? { error: "Empty selection entry" }
		: // quoted identifier
			P extends `"${infer Body}"`
			? // forbid raw " inside quoted body
				Body extends `${string}"${string}`
				? { error: `Invalid \" inside quoted identifier: '${P}'` }
				: Body extends keyof Obj
					? Body
					: { error: `Unknown field: '${Body}'` }
			: // malformed quoting
				P extends `${string}"${string}`
				? { error: `Malformed quoted identifier: '${P}'` }
				: // unquoted identifier
					IsValidUnquoted<P> extends false
					? { error: `Invalid PostgreSQL identifier: '${P}'` }
					: Lowercase<P> extends keyof Obj
						? Lowercase<P>
						: { error: `Unknown field: '${Lowercase<P>}' (from '${P}')` }
	: never;

type ValidateParts<Parts extends readonly string[], Obj> = {
	[I in keyof Parts]: ValidatePart<Parts[I], Obj>;
};

export type ParseSelection<
	S extends string,
	Obj,
> = Split<S> extends infer Parts extends string[]
	? ValidateParts<Parts, Obj> extends infer V extends any[]
		? HasError<V> extends true
			? V[number] // union of actual error messages
			: PickFromKeys<ExtractKeys<V>, Obj>
		: never
	: never;

// suggestions for one entry (best-effort)
type IdToken<K extends string> =
	| Lowercase<K> // unquoted form
	| `"${K}"`; // quoted exact form

// autocomplete applies only to each segment,
// not recursively (fast + stable)
export type SelectionInput<Obj> = string & IdToken<keyof Obj & string>;

export type SelectedRow<Row, S extends string | undefined> = S extends string
	? ParseSelection<S, Row> extends { error: string }
		? never // surface error in config
		: ParseSelection<S, Row>
	: Row; // no selection -> full row
