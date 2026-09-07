package co.kademi.sync.status;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * The reporter decides when something is worth interrupting a person for. Getting that wrong in
 * either direction is the whole risk: silence when a sync is stuck, or a popup per file.
 */
public class SyncStatusReporterTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Records what would have gone to a status bar, so the decisions can be asserted on. */
    private static class Recorder implements StatusSink {

        final List<SyncState> reported = new ArrayList<>();
        final List<String> alerts = new ArrayList<>();
        boolean closed;

        @Override
        public void report(SyncStatus status) {
            reported.add(status.getState());
        }

        @Override
        public boolean alert(String title, String body, boolean problem) {
            alerts.add(title);
            return true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private SyncStatusReporter reporter(Recorder r) throws Exception {
        // built through the real factory so the status file is exercised too, then the recorder
        // is what the assertions read
        SyncStatusReporter reporter = SyncStatusReporter.create("sync", tmp.getRoot(), tmp.newFolder(".ksync"),
                "https://acme.kademi.co/", tmp.getRoot().toPath().resolve("status.json"), false);
        java.lang.reflect.Field f = SyncStatusReporter.class.getDeclaredField("sinks");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<StatusSink> sinks = (List<StatusSink>) f.get(reporter);
        sinks.add(r);
        return reporter;
    }

    @Test
    public void aProblemIsAnnouncedOnce() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);

        reporter.problem(SyncState.BLOCKED, "the remote has changed");
        reporter.problem(SyncState.BLOCKED, "the remote has changed, still");
        reporter.problem(SyncState.BLOCKED, "and again");

        assertEquals("a persisting problem must not notify on every push", 1, r.alerts.size());
        assertTrue(r.alerts.get(0), r.alerts.get(0).contains("pull"));
    }

    @Test
    public void recoveryIsAnnouncedToo() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);

        reporter.problem(SyncState.FAILED, "boom");
        reporter.state(SyncState.IDLE, "pushed");

        assertEquals(2, r.alerts.size());
        assertTrue(r.alerts.get(1), r.alerts.get(1).contains("back to normal"));
    }

    /** The ordinary busy states are the common case and must be silent. */
    @Test
    public void routineWorkIsSilent() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);

        reporter.state(SyncState.SCANNING, null);
        reporter.state(SyncState.PUSHING, "uploading");
        reporter.state(SyncState.IDLE, "pushed");
        reporter.state(SyncState.PULLING, null);
        reporter.state(SyncState.IDLE, "pulled");

        assertEquals(0, r.alerts.size());
        assertEquals(5, r.reported.size());
    }

    /**
     * Exiting does not fix a failure, and whatever reads the file next is better served by the
     * failure than by "stopped".
     */
    @Test
    public void stoppingKeepsAProblemVisible() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);

        reporter.problem(SyncState.FAILED, "boom");
        reporter.stopped();
        assertEquals(SyncState.FAILED, reporter.current().getState());

        reporter.state(SyncState.IDLE, "pushed");
        reporter.stopped();
        assertEquals(SyncState.STOPPED, reporter.current().getState());
        assertEquals("the detail should survive so a one shot push still says what it did",
                "pushed", reporter.current().getDetail());
    }

    /**
     * Being killed halfway through a push is not the same as finishing. Reporting it as a clean
     * stop makes an interrupted push indistinguishable from a completed one, which is how a
     * broken error path went unnoticed once already.
     */
    @Test
    public void stoppingMidOperationIsRecordedAsAFailure() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);

        reporter.state(SyncState.PUSHING, "uploading changed files");
        reporter.stopped();

        assertEquals(SyncState.FAILED, reporter.current().getState());
        assertTrue(reporter.current().getDetail(),
                reporter.current().getDetail().contains("did not finish"));
        assertTrue(reporter.current().getDetail(),
                reporter.current().getDetail().contains("uploading changed files"));
    }

    @Test
    public void closeIsIdempotentAndSilencesFurtherReports() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter reporter = reporter(r);
        reporter.state(SyncState.IDLE, null);
        int before = r.reported.size();

        reporter.close();
        reporter.close();
        reporter.state(SyncState.PUSHING, "too late");

        assertTrue(r.closed);
        assertEquals("nothing should be reported after close", before, r.reported.size());
    }

    /** The commands with nothing to say still get a usable reporter, not null. */
    @Test
    public void theNoOpReporterAcceptsEverything() {
        SyncStatusReporter none = SyncStatusReporter.none();
        none.state(SyncState.PUSHING, "ignored");
        none.problem(SyncState.FAILED, "ignored");
        none.hashes("a", "b");
        none.errorCount(3);
        none.alert("t", "b", true);
        none.stopped();
        none.close();
    }

    @Test
    public void theStatusFileIsWrittenByTheFactory() throws Exception {
        File configDir = tmp.newFolder("cfg");
        SyncStatusReporter reporter = SyncStatusReporter.create("push", tmp.getRoot(), configDir,
                "https://acme.kademi.co/", null, false);
        try {
            assertTrue("the factory should publish the initial status immediately",
                    StatusFile.defaultPath(configDir).toFile().exists());
        } finally {
            reporter.close();
        }
    }
}
