import { Task } from "./Task";

export type RepeatedTaskOptions = {
	cron: string;
};

export class RepeatedTask extends Task {
	override __type = "RepeatedTask";
}
