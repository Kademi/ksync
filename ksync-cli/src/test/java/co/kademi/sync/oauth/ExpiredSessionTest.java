package co.kademi.sync.oauth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * What happens when there is nothing left to refresh with. A dead session and an unreachable
 * server look identical to a naive implementation, but they need opposite advice: one means log
 * in again, the other means try later.
 */
public class ExpiredSessionTest {

    private CredentialStore store;
    private Path storePath;
    private HttpServer server;
    private volatile int tokenStatus = 400;
    private volatile String tokenBody = "{\"error\":\"invalid_grant\",\"error_description\":\"The refresh token has been revoked or is invalid\"}";

    @Before
    public void setUp() throws Exception {
        storePath = Files.createTempDirectory("expired").resolve("credentials.json");
        store = new CredentialStore(storePath);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", (HttpExchange ex) -> {
            byte[] b = tokenBody.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(tokenStatus, b.length);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(b);
            }
        });
        server.start();
    }

    @After
    public void tearDown() {
        server.stop(0);
    }

    private String tokenEndpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/_oauth2/token";
    }

    private OAuth2Client withExpiredSession(String tokenEndpointOverride) throws IOException {
        CredentialStore.Credentials c = new CredentialStore.Credentials();
        c.clientId = "client-1";
        c.accessToken = "ko2-dead";
        c.refreshToken = "dead-refresh";
        c.expiresAt = 1; // long expired
        c.tokenEndpoint = tokenEndpointOverride;
        store.put("acme.kademi.com", c);
        OAuth2Client oauth = new OAuth2Client("https://acme.kademi.com", store);
        oauth.envTokenSource = () -> null;
        return oauth;
    }

    /** Nothing stored at all: say what to run, not what went wrong internally. */
    @Test
    public void neverLoggedInSaysWhatToRun() {
        OAuth2Client oauth = new OAuth2Client("https://acme.kademi.com", store);
        oauth.envTokenSource = () -> null;
        try {
            oauth.accessToken();
            fail("expected NotLoggedInException");
        } catch (NotLoggedInException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Not logged in"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("login --oauth"));
        }
    }

    /** Expired with no refresh token to try: same, and nothing to clean up. */
    @Test
    public void expiredWithNoRefreshTokenSaysWhatToRun() throws Exception {
        CredentialStore.Credentials c = new CredentialStore.Credentials();
        c.accessToken = "ko2-dead";
        c.expiresAt = 1;
        store.put("acme.kademi.com", c);

        OAuth2Client oauth = new OAuth2Client("https://acme.kademi.com", store);
        oauth.envTokenSource = () -> null;
        try {
            oauth.accessToken();
            fail("expected NotLoggedInException");
        } catch (NotLoggedInException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("no refresh"));
        }
    }

    /**
     * The refresh token is dead. The user has to log in again, and the useless credentials must
     * be dropped, or hasSession keeps claiming a session and every later command fails the same
     * way instead of falling back.
     */
    @Test
    public void aRejectedRefreshTokenClearsTheSessionAndSaysToLogIn() throws Exception {
        OAuth2Client oauth = withExpiredSession(tokenEndpoint());
        assertTrue("precondition: looks like a session", oauth.hasSession());

        try {
            oauth.accessToken();
            fail("expected NotLoggedInException");
        } catch (NotLoggedInException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("expired and could not be renewed"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("login --oauth"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("invalid_grant"));
        }

        CredentialStore.Credentials after = store.get("acme.kademi.com");
        assertNull("the dead access token must be gone", after == null ? null : after.accessToken);
        assertNull("the dead refresh token must be gone", after == null ? null : after.refreshToken);
        assertNotNull("but the client registration is worth keeping", after.clientId);

        // and a fresh client agrees there is no session, so ksync falls back cleanly
        OAuth2Client reloaded = new OAuth2Client("https://acme.kademi.com", store);
        reloaded.envTokenSource = () -> null;
        assertFalse(reloaded.hasSession());
    }

    /**
     * The server could not be reached. The credentials may be perfectly good, so they must be
     * kept and the advice must not be "log in again".
     */
    @Test
    public void anUnreachableServerKeepsTheSessionAndDoesNotDemandALogin() throws Exception {
        // port 1 with nothing listening
        OAuth2Client oauth = withExpiredSession("http://127.0.0.1:1/_oauth2/token");

        try {
            oauth.accessToken();
            fail("expected a failure");
        } catch (NotLoggedInException ex) {
            fail("a network problem must not be reported as a lost login: " + ex.getMessage());
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("may be temporary"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("kept"));
        }

        CredentialStore.Credentials after = store.get("acme.kademi.com");
        assertEquals("the refresh token must survive a network failure", "dead-refresh", after.refreshToken);
    }

    /** A 5xx is the server's problem, not the user's login. */
    @Test
    public void aServerErrorIsTreatedAsTemporary() throws Exception {
        tokenStatus = 503;
        tokenBody = "service unavailable";
        OAuth2Client oauth = withExpiredSession(tokenEndpoint());

        try {
            oauth.accessToken();
            fail("expected a failure");
        } catch (NotLoggedInException ex) {
            fail("a 503 must not be reported as a lost login: " + ex.getMessage());
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("may be temporary"));
        }
        assertEquals("dead-refresh", store.get("acme.kademi.com").refreshToken);
    }

    /** An unreadable error body is not evidence the login is gone either. */
    @Test
    public void anUnparseableErrorIsTreatedAsTemporary() throws Exception {
        tokenStatus = 400;
        tokenBody = "<html>a proxy ate your request</html>";
        OAuth2Client oauth = withExpiredSession(tokenEndpoint());

        try {
            oauth.accessToken();
            fail("expected a failure");
        } catch (NotLoggedInException ex) {
            fail("an unreadable error must not be reported as a lost login: " + ex.getMessage());
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("temporary"));
        }
        assertEquals("dead-refresh", store.get("acme.kademi.com").refreshToken);
    }

    /** A client id the server no longer knows must force a fresh registration on next login. */
    @Test
    public void anUnknownClientAlsoClearsTheRegistration() throws Exception {
        tokenStatus = 401;
        tokenBody = "{\"error\":\"invalid_client\",\"error_description\":\"unknown client\"}";
        OAuth2Client oauth = withExpiredSession(tokenEndpoint());

        try {
            oauth.accessToken();
            fail("expected NotLoggedInException");
        } catch (NotLoggedInException expected) {
            // the registration is stale too, so it must not be reused
        }
        CredentialStore.Credentials after = store.get("acme.kademi.com");
        assertTrue("the entry should be gone or have no client id", after == null || after.clientId == null);
    }

    /** The CLI needs to find it through whatever milton wrapped it in. */
    @Test
    public void isFindableThroughAWrappedCause() {
        NotLoggedInException original = new NotLoggedInException("log in");
        Exception wrapped = new RuntimeException("sync failed", new IllegalStateException("inner", original));

        assertEquals(original, NotLoggedInException.find(wrapped));
        assertNull(NotLoggedInException.find(new RuntimeException("unrelated")));
    }
}
