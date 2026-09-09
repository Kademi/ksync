package co.kademi.sync;

import co.kademi.sync.oauth.NotLoggedInException;
import co.kademi.sync.status.StatusSink;
import co.kademi.sync.status.SyncState;
import co.kademi.sync.status.SyncStatus;
import co.kademi.sync.status.SyncStatusReporter;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A background push that fails has to be sorted into two piles: worth waiting out, and not.
 *
 * Getting this wrong either way costs the user their work. Ending the sync on a network blip
 * stops watching for changes over something that would have fixed itself; carrying on after the
 * login has gone leaves a sync that looks alive and pushes nothing.
 */
public class ExpiredSessionEndsSyncTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static class Recorder implements StatusSink {

        final List<SyncStatus> reported = new ArrayList<>();

        @Override
        public void report(SyncStatus status) {
            reported.add(status);
        }

        @Override
        public boolean alert(String title, String body, boolean problem) {
            return true; // swallowed, so the test never tries for a real desktop notification
        }

        @Override
        public void close() {
        }

        SyncStatus last() {
            return reported.get(reported.size() - 1);
        }
    }

    private SyncStatusReporter reporter(Recorder r) throws Exception {
        SyncStatusReporter reporter = SyncStatusReporter.create("sync", tmp.getRoot(), tmp.newFolder(".ksync"),
                "https://acme.kademi.co/", tmp.getRoot().toPath().resolve("status.json"), false);
        java.lang.reflect.Field f = SyncStatusReporter.class.getDeclaredField("sinks");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<StatusSink> sinks = (List<StatusSink>) f.get(reporter);
        sinks.add(r);
        return reporter;
    }

    /**
     * The reported case: the refresh token is dead, so every later push fails identically. The
     * message has to survive into the status, because it names the command that fixes it.
     */
    @Test
    public void anExpiredSessionEndsTheSync() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter status = reporter(r);
        NotLoggedInException ex = new NotLoggedInException("The session for acme has expired. Run: ksync -command login -oauth");

        assertTrue(KSync3.reportPushFailure(status, ex));
        assertEquals(SyncState.FAILED, r.last().getState());
        assertEquals(ex.getMessage(), r.last().getDetail());
    }

    /** Wrapped, which is how it arrives from a push: through getRemoteHash and the http client */
    @Test
    public void anExpiredSessionIsFoundThroughItsWrappers() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter status = reporter(r);
        NotLoggedInException cause = new NotLoggedInException("log in again");

        assertTrue(KSync3.reportPushFailure(status, new IOException("get failed", new RuntimeException(cause))));
    }

    /** An unreachable server is temporary, and the next local change is worth another try */
    @Test
    public void anOfflineServerDoesNotEndTheSync() throws Exception {
        Recorder r = new Recorder();
        SyncStatusReporter status = reporter(r);

        assertFalse(KSync3.reportPushFailure(status, new IOException("push", new ConnectException("Connection refused"))));
        assertEquals(SyncState.OFFLINE, r.last().getState());
    }
}
