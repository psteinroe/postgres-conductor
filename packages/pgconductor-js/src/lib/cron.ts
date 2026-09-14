import CronExpressionParser from "cron-parser";

export function nextCronOccurrence(expression: string, now: Date): Date {
	return CronExpressionParser.parse(expression, { currentDate: now }).next().toDate();
}
