package co.kademi.sync.status;

/**
 * Somewhere status goes. There are two: a JSON file anything can read, and an icon in the OS
 * status bar.
 *
 * Every method must swallow its own failures. A status bar is a courtesy, and a sync must never
 * fall over because a tray icon could not be drawn or a file could not be written.
 */
public interface StatusSink {

    void report(SyncStatus status);

    /**
     * A transient message about something that just happened and will not be visible later - a
     * push that failed, a remote that was overwritten.
     *
     * @param problem whether this is bad news, which some platforms show differently
     * @return true if it was handed to something that will actually show it, so the caller knows
     * whether to try another route
     */
    boolean alert(String title, String body, boolean problem);

    void close();
}
