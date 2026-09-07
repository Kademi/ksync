package co.kademi.sync.status;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.sf.json.JSONObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StatusFileTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static SyncStatus status() {
        return SyncStatus.initial("sync", "/home/brad/site", "https://acme.kademi.co/");
    }

    /** Inside .ksync, which the scanner ignores by name, or the file would sync itself in a loop. */
    @Test
    public void defaultsInsideTheConfigDir() {
        Path p = StatusFile.defaultPath(new File("/home/brad/site/.ksync"));
        assertEquals("status.json", p.getFileName().toString());
        assertEquals(".ksync", p.getParent().getFileName().toString());
    }

    /** -statusfile status.json, with no directory, used to fail on every write: no parent to put the temp file in */
    @Test
    public void aBareFileNameResolvesAgainstTheWorkingDirectory() {
        StatusFile f = new StatusFile(Paths.get("status.json"));
        assertTrue(f.getPath().isAbsolute());
        assertEquals(Paths.get("").toAbsolutePath(), f.getPath().getParent());
    }

    @Test
    public void writesParseableJson() throws IOException {
        Path path = tmp.getRoot().toPath().resolve("status.json");
        StatusFile f = new StatusFile(path);
        f.report(status().withState(SyncState.PUSHING, "uploading"));

        JSONObject parsed = JSONObject.fromObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
        assertEquals("PUSHING", parsed.getString("state"));
        assertEquals("uploading", parsed.getString("detail"));
        assertTrue(parsed.getLong("pid") > 0);
    }

    @Test
    public void replacesTheFileOnEachReport() throws IOException {
        Path path = tmp.getRoot().toPath().resolve("status.json");
        StatusFile f = new StatusFile(path);
        f.report(status().withState(SyncState.PUSHING, null));
        f.report(status().withState(SyncState.IDLE, "pushed"));

        JSONObject parsed = JSONObject.fromObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
        assertEquals("IDLE", parsed.getString("state"));
    }

    /** The temp file is written beside the target and must not be left lying around. */
    @Test
    public void leavesNoTempFilesBehind() throws IOException {
        Path path = tmp.getRoot().toPath().resolve("status.json");
        StatusFile f = new StatusFile(path);
        for (int i = 0; i < 5; i++) {
            f.report(status().withState(SyncState.SCANNING, "pass " + i));
        }
        String[] names = tmp.getRoot().list();
        assertEquals("only the status file should remain: " + String.join(", ", names), 1, names.length);
        assertEquals("status.json", names[0]);
    }

    /** Missing parent directories are created rather than reported as a failure. */
    @Test
    public void createsMissingDirectories() throws IOException {
        Path path = tmp.getRoot().toPath().resolve("nested/deeper/status.json");
        new StatusFile(path).report(status());
        assertTrue(Files.exists(path));
    }

    /**
     * A status file is a courtesy. An unwritable path must be logged and shrugged off, never
     * thrown, or a sync would die because a status bar could not be updated.
     */
    @Test
    public void anUnwritablePathDoesNotThrow() throws IOException {
        File dir = tmp.newFolder("readonly");
        StatusFile f = new StatusFile(dir.toPath().resolve("sub/status.json"));
        assertTrue(dir.setWritable(false));
        try {
            f.report(status());
            f.report(status().withState(SyncState.IDLE, "second time, now via the debug path"));
        } finally {
            dir.setWritable(true);
        }
        assertFalse(Files.exists(dir.toPath().resolve("sub/status.json")));
    }

    @Test
    public void aFileCannotShowAnAlert() {
        assertFalse(new StatusFile(tmp.getRoot().toPath().resolve("s.json")).alert("t", "b", true));
    }
}
