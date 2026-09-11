package co.kademi.sync;

/**
 * The shape of each log line.
 *
 * An enum rather than a string so the parser rejects a typo by name, and so the generated
 * completion and man page list the choices without anyone maintaining a second copy of them.
 */
public enum LogFormat {
    /** The message alone, which is what a person reading a terminal wants. */
    PLAIN,
    /** An ISO-8601 UTC timestamp and level first, for a log file. */
    TS,
    /** ts=.. level=.. msg=".." , for a log reader that parses fields. */
    KV
}
