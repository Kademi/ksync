package co.kademi.sync;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;

/**
 * How a checkout decides whether to ask the repository which version it belongs on, and what it
 * records afterwards.
 */
public class VersionTrackingPropsTest {

    private static final String REPO = "https://acme.kademi.us/repositories/KProducts/";
    private static final String V146 = REPO + "1.4.6/";
    private static final String V147 = REPO + "1.4.7/";

    private File configDir;

    @Before
    public void setUp() throws Exception {
        configDir = Files.createTempDirectory("ksync").resolve(".ksync").toFile();
        configDir.mkdirs();
    }

    private Properties props(String... keyValues) {
        Properties props = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            props.put(keyValues[i], keyValues[i + 1]);
        }
        return props;
    }

    private String prop(String key) {
        return KSyncUtils.readProps(configDir).getProperty(key);
    }

    @Test
    public void aVersionUrlItHasUsedBefore_asksNothing() {
        // the common case: an existing checkout must not pay a request to be told what it knows
        assertEquals(RepoMeta.Tracking.OFF, KSyncUtils.trackingFor(V146, props("url", V146)));
    }

    @Test
    public void aKnownRepository_isResolvedEveryRun() {
        assertEquals(RepoMeta.Tracking.TRACK, KSyncUtils.trackingFor(REPO, props("url", V146, "repoUrl", REPO)));
    }

    @Test
    public void aNewOrChangedUrl_isProbed() {
        assertEquals(RepoMeta.Tracking.PROBE, KSyncUtils.trackingFor(REPO, new Properties()));
        assertEquals(RepoMeta.Tracking.PROBE, KSyncUtils.trackingFor(V147, props("url", V146)));
    }

    @Test
    public void followingARepository_recordsBothUrls() {
        KSyncUtils.writeProps(V147, "someuser", REPO, configDir);
        assertEquals(V147, prop("url"));
        assertEquals(REPO, prop("repoUrl"));
    }

    @Test
    public void aVersionChangeWithinTheRepository_keepsTheRecordedHash() {
        // the hash is the head of the version being left, which is the base the next pull needs to
        // work out what changed between the two versions
        KSyncUtils.writeProps(V146, "someuser", REPO, configDir);
        KSyncUtils.saveRemoteHash(configDir, "hash-of-146");
        KSyncUtils.writeProps(V147, "someuser", REPO, configDir);
        assertEquals("hash-of-146", KSyncUtils.getLastRemoteHash(configDir));
    }

    @Test
    public void pinningAnExistingCheckoutToARepository_keepsTheRecordedHash() {
        // it was already a version of this repository, so the same relationship holds
        KSyncUtils.writeProps(V146, "someuser", configDir);
        KSyncUtils.saveRemoteHash(configDir, "hash-of-146");
        KSyncUtils.writeProps(V147, "someuser", REPO, configDir);
        assertEquals("hash-of-146", KSyncUtils.getLastRemoteHash(configDir));
    }

    @Test
    public void repointingAtAnUnrelatedRepository_dropsTheRecordedHash() {
        KSyncUtils.writeProps(V146, "someuser", REPO, configDir);
        KSyncUtils.saveRemoteHash(configDir, "hash-of-146");
        KSyncUtils.writeProps("https://acme.kademi.us/repositories/KSupport/2.0.0/", "someuser",
                "https://acme.kademi.us/repositories/KSupport/", configDir);
        assertNull(KSyncUtils.getLastRemoteHash(configDir));
    }

    @Test
    public void pinningToAVersion_stopsFollowingTheRepository() {
        KSyncUtils.writeProps(V147, "someuser", REPO, configDir);
        KSyncUtils.writeProps(V146, "someuser", null, configDir);
        assertEquals(V146, prop("url"));
        assertNull(prop("repoUrl"));
    }

    @Test
    public void theLoginCommand_doesNotStopTheCheckoutFollowingARepository() {
        // it writes the url and nothing else; forgetting the repository here would silently pin it
        KSyncUtils.writeProps(V147, "someuser", REPO, configDir);
        KSyncUtils.writeProps(V147, null, configDir);
        assertEquals(REPO, prop("repoUrl"));
    }
}
