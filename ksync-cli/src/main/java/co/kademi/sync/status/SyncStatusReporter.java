package co.kademi.sync.status;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The front door for status: ksync tells this what it is doing, and it fans that out to the
 * status file and, when there is one, the status bar icon.
 *
 * It exists because a sync driven by a coding assistant has no visible UI at all - nothing says
 * whether it is connected, working, or stuck waiting for a pull.
 *
 * Callers never need a null check: {@link #none()} is a working reporter that goes nowhere, for
 * the commands that have no status worth publishing.
 */
public class SyncStatusReporter {

    private static final Logger log = LoggerFactory.getLogger(SyncStatusReporter.class);

    private final List<StatusSink> sinks;
    private SyncStatus current;
    private boolean closed;

    /**
     * @param statusFile where to write the JSON, or null for the default inside .ksync
     * @param wantTray whether to try for a status bar icon. Only worth it for a long running
     * sync: on a command that finishes in half a second the icon would appear and vanish before
     * it could be read, having paid for AWT startup on the way
     */
    public static SyncStatusReporter create(String command, File localDir, File configDir, String url,
            Path statusFile, boolean wantTray) {
        List<StatusSink> sinks = new ArrayList<>();
        sinks.add(new StatusFile(statusFile != null ? statusFile : StatusFile.defaultPath(configDir)));
        if (wantTray) {
            TrayStatusIcon tray = TrayStatusIcon.createOrNull();
            if (tray != null) {
                sinks.add(tray);
            }
        }
        SyncStatusReporter reporter = new SyncStatusReporter(sinks,
                SyncStatus.initial(command, localDir.getAbsolutePath(), url));
        reporter.publish();
        reporter.addShutdownHook();
        return reporter;
    }

    /** A reporter that publishes nowhere, so callers can be written without conditionals */
    public static SyncStatusReporter none() {
        return new SyncStatusReporter(Collections.emptyList(),
                SyncStatus.initial(null, ".", null).withState(SyncState.STOPPED, null));
    }

    private SyncStatusReporter(List<StatusSink> sinks, SyncStatus initial) {
        this.sinks = sinks;
        this.current = initial;
    }

    /**
     * A sync runs until it is killed, usually with ctrl-c, so without this the status file would
     * be left saying the sync is idle long after the process is gone and the icon would linger.
     */
    private void addShutdownHook() {
        if (sinks.isEmpty()) {
            return;
        }
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                stopped();
                close();
            }, "ksync-status-shutdown"));
        } catch (RuntimeException ex) {
            log.debug("Could not register the status shutdown hook", ex);
        }
    }

    public synchronized SyncStatus current() {
        return current;
    }

    public synchronized void state(SyncState state, String detail) {
        if (closed) {
            return;
        }
        SyncState previous = current.getState();
        current = current.withState(state, detail);
        publish();
        announceTransition(previous, state);
    }

    /**
     * Reports that ksync itself is up and watching. Unlike the idle that follows a completed push
     * or pull, this says nothing about the server, so it must not overwrite a problem: a status
     * icon showing "up to date" while the server is unreachable is worse than no icon at all.
     */
    public synchronized void ready(String detail) {
        if (closed || current.getState().isProblem()) {
            log.debug("Not reporting ready, because the status is {}", current.getState());
            return;
        }
        state(SyncState.IDLE, detail);
    }

    /** Records a problem, which also carries the message into the status as the last error */
    public synchronized void problem(SyncState state, String message) {
        if (closed) {
            return;
        }
        SyncState previous = current.getState();
        current = current.withError(state, message);
        publish();
        announceTransition(previous, state);
    }

    public synchronized void hashes(String localHash, String remoteHash) {
        if (closed) {
            return;
        }
        current = current.withHashes(localHash, remoteHash);
        publish();
    }

    public synchronized void errorCount(int count) {
        if (closed || count == current.getErrorCount()) {
            return;
        }
        current = current.withErrorCount(count);
        publish();
    }

    /**
     * Notifies about something that just happened and would otherwise leave no trace on screen.
     * Falls back to the log when there is nothing to show it on, so the message is never simply
     * lost.
     */
    public synchronized void alert(String title, String body, boolean problem) {
        for (StatusSink sink : sinks) {
            try {
                if (sink.alert(title, body, problem)) {
                    return;
                }
            } catch (RuntimeException ex) {
                log.debug("Sink {} could not alert", sink.getClass().getSimpleName(), ex);
            }
        }
        if (Notifier.send(title, body, problem)) {
            return;
        }
        log.debug("Nowhere to show a notification: {} - {}", title, body);
    }

    /**
     * Only the edges are worth a notification: going wrong, and coming right again. Notifying on
     * every report would mean a popup per file during a large push, and repeating one while a
     * problem persists is how people learn to ignore notifications.
     */
    private void announceTransition(SyncState previous, SyncState next) {
        if (previous == next) {
            return;
        }
        if (next.isProblem()) {
            alert("ksync: " + next.getLabel(), current.getDetail() == null ? next.getLabel() : current.getDetail(), true);
        } else if (previous.isProblem() && next == SyncState.IDLE) {
            alert("ksync: back to normal", current.summary(), false);
        }
    }

    private void publish() {
        for (StatusSink sink : sinks) {
            try {
                sink.report(current);
            } catch (RuntimeException ex) {
                log.debug("Sink {} could not report", sink.getClass().getSimpleName(), ex);
            }
        }
    }

    /**
     * The final state as the process exits.
     *
     * A problem is left standing: the process ending does not resolve it, and "failed, and here is
     * why" is more use to whoever reads this next than "stopped".
     *
     * Dying midway through an operation is recorded as a failure rather than a clean stop, which
     * is the honest reading - the push did not finish - and it is a net under any path that exits
     * without reporting why. A bare "stopped" after a half done push looks exactly like a push
     * that completed, which is how a broken error path stayed invisible once already.
     */
    public synchronized void stopped() {
        if (closed || current.getState().isProblem()) {
            return;
        }
        if (current.getState().isBusy()) {
            String was = current.getDetail() == null ? current.getState().getLabel() : current.getDetail();
            current = current.withError(SyncState.FAILED, "did not finish: " + was);
        } else {
            current = current.withState(SyncState.STOPPED, current.getDetail());
        }
        publish();
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (StatusSink sink : sinks) {
            try {
                sink.close();
            } catch (RuntimeException ex) {
                log.debug("Sink {} could not close", sink.getClass().getSimpleName(), ex);
            }
        }
    }
}
