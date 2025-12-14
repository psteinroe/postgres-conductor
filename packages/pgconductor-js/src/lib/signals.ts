export const SIGNALS = ["SIGUSR2", "SIGINT", "SIGTERM", "SIGPIPE", "SIGHUP", "SIGABRT"] as const;

export type Signal = (typeof SIGNALS)[number];
