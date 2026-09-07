package co.kademi.sync.status;

import net.sf.json.JSONObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * The JSON is a contract with whatever reads the status file - a status bar, an editor, a shell
 * prompt - so what matters here is that it stays parseable whatever ends up in a file name or an
 * error message, and that the states classify themselves the way an icon expects.
 */
public class SyncStatusTest {

    private static SyncStatus status() {
        return SyncStatus.initial("sync", "/home/brad/proj/site", "https://acme.kademi.co/repositories/w1/version1/");
    }

    @Test
    public void carriesTheStateAndDetail() {
        SyncStatus s = status().withState(SyncState.PUSHING, "uploading changed files");
        assertEquals(SyncState.PUSHING, s.getState());
        assertTrue(s.toJson().contains("\"state\": \"PUSHING\""));
        assertTrue(s.toJson().contains("\"detail\": \"uploading changed files\""));
        assertEquals("Pushing local changes - uploading changed files", s.summary());
    }

    /** An absent value has to be JSON null, not the string "null" and not a missing key. */
    @Test
    public void absentValuesAreJsonNull() {
        String json = status().toJson();
        assertTrue(json, json.contains("\"detail\": null"));
        assertTrue(json, json.contains("\"lastError\": null"));
        assertTrue(json, json.contains("\"localHash\": null"));
    }

    /**
     * The detail can be an error message or a file path, and either can hold a quote, a newline
     * or a control character. Unescaped, one would end the string early and hand the reader
     * broken JSON.
     *
     * Checked by parsing the output with a real parser rather than by matching substrings: the
     * question is whether a reader can get the value back, not whether it looks escaped.
     */
    @Test
    public void awkwardTextSurvivesARoundTrip() {
        String nasty = "could not read \"my file\".txt\nline two\ttabbed \\ and a bell \u0007";
        String json = status().withError(SyncState.FAILED, nasty).toJson();

        JSONObject parsed = JSONObject.fromObject(json);
        assertEquals(nasty, parsed.getString("detail"));
        assertEquals(nasty, parsed.getString("lastError"));
        assertEquals("FAILED", parsed.getString("state"));
        assertEquals(1, parsed.getInt("errorCount"));
        assertTrue(parsed.getBoolean("problem"));
    }

    @Test
    public void aWindowsPathSurvivesARoundTrip() {
        String path = "C:\\Users\\brad\\site";
        JSONObject parsed = JSONObject.fromObject(status().withState(SyncState.PULLING, path).toJson());
        assertEquals(path, parsed.getString("detail"));
        assertEquals(path, JSONObject.fromObject(
                SyncStatus.initial("sync", path, null).toJson()).getString("localDir"));
    }

    @Test
    public void errorsAccumulateAndSurviveRecovery() {
        SyncStatus s = status().withError(SyncState.FAILED, "boom");
        assertEquals(1, s.getErrorCount());
        SyncStatus recovered = s.withState(SyncState.IDLE, "pushed");
        assertEquals(SyncState.IDLE, recovered.getState());
        assertEquals("the last error is still worth reporting after recovery", 1, recovered.getErrorCount());
        assertTrue(recovered.toJson().contains("\"lastError\": \"boom\""));
    }

    @Test
    public void hashesOnlyOverwriteWhenGiven() {
        SyncStatus s = status().withHashes("aaa", "bbb").withHashes(null, "ccc");
        assertTrue(s.toJson().contains("\"localHash\": \"aaa\""));
        assertTrue(s.toJson().contains("\"remoteHash\": \"ccc\""));
    }

    /**
     * The icon and any reader key off these two, so a new state must land in the right bucket.
     * Blocked is the interesting one: ksync is working correctly, but it needs a person.
     */
    @Test
    public void statesClassifyThemselves() {
        assertTrue(SyncState.BLOCKED.isProblem());
        assertTrue(SyncState.OFFLINE.isProblem());
        assertTrue(SyncState.FAILED.isProblem());
        assertFalse(SyncState.IDLE.isProblem());
        assertFalse(SyncState.STOPPED.isProblem());
        assertFalse("blocked is stuck, not working", SyncState.BLOCKED.isBusy());
        assertTrue(SyncState.PUSHING.isBusy());
        assertTrue(SyncState.PULLING.isBusy());
        assertFalse(SyncState.IDLE.isBusy());
    }

    @Test
    public void everyStateHasALabelAndSerialises() {
        for (SyncState state : SyncState.values()) {
            assertFalse(state.name(), state.getLabel().isEmpty());
            String json = status().withState(state, null).toJson();
            assertTrue(state.name(), json.contains("\"state\": \"" + state.name() + "\""));
            assertTrue(state.name(), json.contains("\"busy\": " + state.isBusy()));
            assertTrue(state.name(), json.contains("\"problem\": " + state.isProblem()));
        }
    }
}
