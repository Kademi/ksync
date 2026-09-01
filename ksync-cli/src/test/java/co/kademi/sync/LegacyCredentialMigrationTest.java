package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Existing checkouts have a cookie login sitting in .ksync/ksync.properties. Moving it into the
 * per-user store must not lose it, and must not leave a copy behind in the checkout.
 */
public class LegacyCredentialMigrationTest {

    private static final String URL = "https://acme.kademi.com/repo/branch";

    private File repoDir;
    private Path storePath;

    @Before
    public void setUp() throws Exception {
        repoDir = Files.createTempDirectory("ksync-migrate").resolve(".ksync").toFile();
        repoDir.mkdirs();
        // Point the default store at a throwaway file. Overriding user.home is not enough:
        // userConfigDir consults XDG_CONFIG_HOME first, so on a machine which sets it the test
        // would write to the developer's real credentials.
        storePath = Files.createTempDirectory("ksync-store").resolve("credentials.json");
        System.setProperty(CredentialStore.PATH_PROPERTY, storePath.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(CredentialStore.PATH_PROPERTY);
    }

    private CredentialStore store() {
        return CredentialStore.defaultStore();
    }

    private void writeLegacyProps(String... kv) throws Exception {
        Properties props = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            props.setProperty(kv[i], kv[i + 1]);
        }
        try (FileOutputStream out = new FileOutputStream(new File(repoDir, "ksync.properties"))) {
            props.store(out, null);
        }
    }

    private Properties readProps() {
        return KSyncUtils.readProps(repoDir);
    }

    @Test
    public void movesALegacyLoginIntoTheStoreAndStripsItFromTheCheckout() throws Exception {
        writeLegacyProps("url", URL, "user", "bob",
                "userUrl", "/users/bob/", "userUrlHash", "the-hash");

        KSyncUtils.migrateLegacyCredentials(URL, repoDir);

        CredentialStore.Credentials creds = store().get("acme.kademi.com");
        assertEquals("/users/bob/", creds.userUrl);
        assertEquals("the-hash", creds.userUrlHash);

        Properties after = readProps();
        assertNull("the secret must not be left in the checkout", after.getProperty("userUrlHash"));
        assertNull(after.getProperty("userUrl"));
        assertEquals("everything else is left alone", URL, after.getProperty("url"));
        assertEquals("bob", after.getProperty("user"));

        String onDisk = new String(Files.readAllBytes(new File(repoDir, "ksync.properties").toPath()), StandardCharsets.UTF_8);
        assertFalse(onDisk, onDisk.contains("the-hash"));
    }

    /** After migrating, the login has to actually work, or we have logged the user out. */
    @Test
    public void theMigratedLoginIsReadableAsCookies() throws Exception {
        writeLegacyProps("url", URL, "userUrl", "/users/bob/", "userUrlHash", "the-hash");

        KSyncUtils.migrateLegacyCredentials(URL, repoDir);

        Map<String, String> cookies = KSyncUtils.getCookies(URL);
        assertEquals("/users/bob/", cookies.get("miltonUserUrl"));
        assertEquals("the-hash", cookies.get("miltonUserUrlHash"));
    }

    @Test
    public void doingItTwiceIsHarmless() throws Exception {
        writeLegacyProps("url", URL, "userUrl", "/users/bob/", "userUrlHash", "the-hash");

        KSyncUtils.migrateLegacyCredentials(URL, repoDir);
        KSyncUtils.migrateLegacyCredentials(URL, repoDir);

        assertEquals("the-hash", store().get("acme.kademi.com").userUrlHash);
    }

    /** A second checkout must not overwrite a fresher login already in the store. */
    @Test
    public void anExistingStoredLoginWins() throws Exception {
        CredentialStore.Credentials existing = new CredentialStore.Credentials();
        existing.userUrl = "/users/bob/";
        existing.userUrlHash = "newer-hash";
        store().put("acme.kademi.com", existing);

        writeLegacyProps("url", URL, "userUrl", "/users/bob/", "userUrlHash", "older-hash");
        KSyncUtils.migrateLegacyCredentials(URL, repoDir);

        assertEquals("newer-hash", store().get("acme.kademi.com").userUrlHash);
        assertNull("the stale copy is still cleared out", readProps().getProperty("userUrlHash"));
    }

    /** An OAuth2 session for the same host must survive a cookie migration. */
    @Test
    public void doesNotDisturbAnOauthSession() throws Exception {
        CredentialStore.Credentials oauth = new CredentialStore.Credentials();
        oauth.clientId = "client-1";
        oauth.accessToken = "ko2-abc";
        oauth.refreshToken = "refresh-1";
        store().put("acme.kademi.com", oauth);

        writeLegacyProps("url", URL, "userUrl", "/users/bob/", "userUrlHash", "the-hash");
        KSyncUtils.migrateLegacyCredentials(URL, repoDir);

        CredentialStore.Credentials creds = store().get("acme.kademi.com");
        assertEquals("ko2-abc", creds.accessToken);
        assertEquals("refresh-1", creds.refreshToken);
        assertEquals("the-hash", creds.userUrlHash);
    }

    @Test
    public void nothingToMigrateIsANoOp() throws Exception {
        writeLegacyProps("url", URL, "user", "bob");
        KSyncUtils.migrateLegacyCredentials(URL, repoDir);
        assertNull(store().get("acme.kademi.com"));
    }

    /**
     * Without a url there is no host to key the login by, so it stays put rather than being
     * silently dropped.
     */
    @Test
    public void withoutAUrlTheLoginIsLeftAlone() throws Exception {
        writeLegacyProps("userUrl", "/users/bob/", "userUrlHash", "the-hash");

        KSyncUtils.migrateLegacyCredentials(null, repoDir);

        assertEquals("the-hash", readProps().getProperty("userUrlHash"));
    }

    /** Changing user invalidates the saved login, which now lives in the store. */
    @Test
    public void changingUserClearsTheStoredLogin() throws Exception {
        writeLegacyProps("url", URL, "user", "bob", "userUrl", "/users/bob/", "userUrlHash", "the-hash");
        KSyncUtils.migrateLegacyCredentials(URL, repoDir);
        assertEquals("the-hash", store().get("acme.kademi.com").userUrlHash);

        KSyncUtils.writeProps(URL, "alice", repoDir);

        assertTrue("the previous user's login must not be reused",
                KSyncUtils.getCookies(URL).isEmpty());
    }
}
