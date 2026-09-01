package co.kademi.sync.oauth;

import java.nio.file.Files;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * KSYNC_TOKEN carries a KOAuth2 api key (ko2_ak_...) for CI, where there is no browser to run a
 * login in. It is used exactly as given: no refresh, and nothing written to disk.
 */
public class EnvTokenTest {

    private CredentialStore store;

    @Before
    public void setUp() throws Exception {
        store = new CredentialStore(Files.createTempDirectory("envtoken").resolve("credentials.json"));
    }

    @Test
    public void anApiKeyFromTheEnvironmentIsASession() throws Exception {
        assertFalse("nothing stored and nothing in the env", clientWithEnv(null).hasSession());

        OAuth2Client oauth = clientWithEnv("ko2_ak_theapikey");
        assertTrue(oauth.hasSession());
        assertEquals("ko2_ak_theapikey", oauth.accessToken());
    }

    @Test
    public void theApiKeyIsNeverWrittenToDisk() throws Exception {
        clientWithEnv("ko2_ak_theapikey").accessToken();

        assertFalse("the credential store must stay untouched", Files.exists(store.getPath()));
    }

    /** An explicit token wins, so CI can override whatever a developer happens to have stored. */
    @Test
    public void theEnvironmentWinsOverAStoredSession() throws Exception {
        CredentialStore.Credentials c = new CredentialStore.Credentials();
        c.accessToken = "ko2-stored";
        c.refreshToken = "stored-refresh";
        c.expiresAt = System.currentTimeMillis() + 3_600_000;
        store.put("acme.kademi.com", c);

        assertEquals("ko2-stored", clientWithEnv(null).accessToken());
        assertEquals("ko2_ak_theapikey", clientWithEnv("ko2_ak_theapikey").accessToken());
    }

    @Test
    public void blankIsIgnored() throws Exception {
        assertFalse(clientWithEnv("   ").hasSession());
    }

    @Test
    public void noSessionAndNoTokenSaysWhatToRun() throws Exception {
        try {
            clientWithEnv(null).accessToken();
            org.junit.Assert.fail("expected to be told to log in");
        } catch (NotLoggedInException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("login"));
            assertTrue(ex.getMessage(), ex.getMessage().contains(OAuth2Client.TOKEN_ENV_VAR));
        }
    }

    private OAuth2Client clientWithEnv(String token) {
        OAuth2Client c = new OAuth2Client("https://acme.kademi.com", store);
        c.envTokenSource = () -> token;
        return c;
    }
}
