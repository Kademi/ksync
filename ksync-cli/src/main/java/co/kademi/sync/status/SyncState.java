package co.kademi.sync.status;

/**
 * What ksync is doing, coarse enough to fit in a status icon.
 *
 * These are deliberately few. A status bar has room for one glyph and a tooltip, and the person
 * reading it wants one of three answers: is it working, is it busy, or does it need me. Anything
 * finer belongs in the detail string or the log.
 */
public enum SyncState {

    STARTING("Starting"),
    SCANNING("Scanning for local changes"),
    IDLE("Up to date"),
    PUSHING("Pushing local changes"),
    PULLING("Pulling from the server"),
    /**
     * The remote moved on and we will not overwrite it. Not a failure - ksync is working
     * correctly - but it will not resolve itself, so it reads as needing attention. This is the
     * state -localwins exists to avoid.
     */
    BLOCKED("Remote has changed, a pull is needed"),
    OFFLINE("Cannot reach the server"),
    FAILED("Last operation failed"),
    STOPPED("Stopped");

    private final String label;

    SyncState(String label) {
        this.label = label;
    }

    /** Human readable, for a tooltip or a status bar */
    public String getLabel() {
        return label;
    }

    /**
     * Whether this state wants a person. Drives the icon colour and whether entering the state
     * is worth a desktop notification.
     */
    public boolean isProblem() {
        return this == BLOCKED || this == OFFLINE || this == FAILED;
    }

    /** Whether work is in progress, so a status bar may want to show it as active */
    public boolean isBusy() {
        return this == STARTING || this == SCANNING || this == PUSHING || this == PULLING;
    }
}
