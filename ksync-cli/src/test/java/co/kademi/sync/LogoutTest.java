package co.kademi.sync;

import co.kademi.sync.commands.LogoutCommand;
import co.kademi.sync.oauth.CredentialStore;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LogoutTest {

    private Path file;

    @Before
    public void setUp() throws Exception {
        file = Files.createTempDirectory("ksync-logout").resolve("credentials.json");
        System.setProperty(CredentialStore.PATH_PROPERTY, file.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(CredentialStore.PATH_PROPERTY);
    }

    /** A person types the domain, a checkout holds a full branch url, and both name one login. */
    @Test
    public void anyWayOfNamingTheSite_findsTheSameLogin() {
        assertEquals("https://acme.kademi.co", KSyncUtils.siteUrl("acme.kademi.co"));
        assertEquals("https://acme.kademi.co", KSyncUtils.siteUrl("https://acme.kademi.co"));
        assertEquals("https://acme.kademi.co", KSyncUtils.siteUrl("  acme.kademi.co  "));
        assertEquals("https://acme.kademi.co",
                KSyncUtils.siteUrl("https://acme.kademi.co/repositories/mysite/version1"));
        // a dev instance on another port is a different site, so the port is kept
        assertEquals("http://localhost:8080", KSyncUtils.siteUrl("http://localhost:8080/repositories/x"));
    }

    /**
     * Both ways in go, because they are two routes to the same account. The client registration
     * stays: it identifies this install, not the person, and re-registering costs a round trip.
     */
    @Test
    public void logout_discardsBothKindsOfCredential_butNotTheRegistration() throws Exception {
        CredentialStore store = new CredentialStore(file);
        CredentialStore.Credentials creds = new CredentialStore.Credentials();
        creds.clientId = "cid-1";
        creds.accessToken = "at-1";
        creds.refreshToken = "rt-1";
        creds.userUrl = "/users/wesley";
        creds.userUrlHash = "hash-1";
        store.put("acme.kademi.co", creds);

        LogoutCommand cmd = new LogoutCommand();
        cmd.url = "acme.kademi.co";
        KSync3.logout(cmd);

        CredentialStore.Credentials after = new CredentialStore(file).get("acme.kademi.co");
        assertNotNull("the client registration should survive a logout", after);
        assertEquals("cid-1", after.clientId);
        assertFalse("still logged in after a logout", after.hasLogin());
        assertNull(after.refreshToken);
        assertNull(after.userUrl);
        assertNull(after.userUrlHash);
    }

    @Test
    public void logout_leavesOtherSitesAlone() throws Exception {
        CredentialStore store = new CredentialStore(file);
        CredentialStore.Credentials keep = new CredentialStore.Credentials();
        keep.accessToken = "at-2";
        store.put("other.kademi.co", keep);

        LogoutCommand cmd = new LogoutCommand();
        cmd.url = "acme.kademi.co";
        KSync3.logout(cmd);

        assertEquals("at-2", new CredentialStore(file).get("other.kademi.co").accessToken);
    }
}
