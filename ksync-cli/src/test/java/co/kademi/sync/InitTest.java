package co.kademi.sync;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;

/**
 * init sets a directory up to sync without downloading it, so the two things it must get right
 * are what counts as already set up, and leaving a directory that is not set up alone.
 */
public class InitTest {

    private static final String REPO = "https://acme.kademi.us/repositories/KProducts/";
    private static final String V146 = REPO + "1.4.6/";

    private File parent;
    private File configDir;

    /**
     * The directory is named the way a ksync:// link names one, with --appdir and --appname,
     * because that is the only way to run a command anywhere but the process working directory.
     */
    @Before
    public void setUp() throws Exception {
        parent = Files.createTempDirectory("ksync-init").toFile();
        configDir = new File(parent, "proj/.ksync");
        configDir.mkdirs();
    }

    private String[] init(String... args) {
        String[] all = new String[args.length + 5];
        all[0] = "init";
        all[1] = "--appdir";
        all[2] = parent.getAbsolutePath();
        all[3] = "--appname";
        all[4] = "proj";
        System.arraycopy(args, 0, all, 5, args.length);
        return all;
    }

    private Properties props(String... keyValues) {
        Properties props = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            props.put(keyValues[i], keyValues[i + 1]);
        }
        return props;
    }

    @Test
    public void aFollowedRepository_isWhatTheDirectoryIsSetUpFor() {
        // not the version under it: that is where following has landed for now, not what was asked for
        assertEquals(REPO, KSyncUtils.targetUrl(props("url", V146, "repoUrl", REPO)));
    }

    @Test
    public void aPinnedCheckout_isSetUpForItsVersion() {
        assertEquals(V146, KSyncUtils.targetUrl(props("url", V146)));
    }

    @Test
    public void anEmptyPropertiesFile_isNotSetUpAtAll() {
        assertNull(KSyncUtils.targetUrl(props()));
        assertNull(KSyncUtils.targetUrl(props("url", "  ")));
    }

    /**
     * A directory already pointed somewhere is reported, not re-done. The url here is a port
     * nothing listens on, so a run which went to the server for anything would fail instead of
     * returning 0 - which is the point: being already set up is answered from disk.
     */
    @Test
    public void anAlreadySetUpDirectory_asksTheServerNothing() throws Exception {
        KSyncUtils.writeProps(props("url", "http://127.0.0.1:1/repositories/x/v1/"), configDir);

        assertEquals(0, Cli.run(init()));

        // and it is left exactly as it was: nothing re-resolved, nothing re-recorded
        assertEquals("http://127.0.0.1:1/repositories/x/v1/", KSyncUtils.readProps(configDir).getProperty("url"));
    }

    /**
     * The url a setup failed on must not be recorded. Every later command in the directory takes
     * its url from that file, so a .ksync naming a branch that never answered is worse than none.
     */
    @Test
    public void aServerThatIsNotThere_recordsNothing() throws Exception {
        assertEquals(1, Cli.run(init("--url", "http://127.0.0.1:1/repositories/x/v1/",
                "-u", "someone", "-p", "secret")));

        assertNull(KSyncUtils.readProps(configDir).getProperty("url"));
    }
}
