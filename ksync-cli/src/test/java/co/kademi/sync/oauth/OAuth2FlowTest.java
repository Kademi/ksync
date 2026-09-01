package co.kademi.sync.oauth;

import co.kademi.sync.BearerHost;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.milton.common.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives the whole authorization code + PKCE flow against a stand-in for the KOAuth2 app,
 * built to the same contract as apps/src/marketplace/apps/KOAuth2: discovery, dynamic client
 * registration for a public client, mandatory S256 PKCE, rotating refresh tokens, and a
 * "ko2-" prefixed bearer token on protected resources.
 */
public class OAuth2FlowTest {

    private void expire(long at) throws IOException {
        CredentialStore.Credentials c = stored();
        c.expiresAt = at;
        store.put(server.baseUrl(), c);
    }


    private FakeAuthServer server;
    private CredentialStore store;
    private java.nio.file.Path storePath;

    @Before
    public void setUp() throws Exception {
        server = new FakeAuthServer();
        storePath = Files.createTempDirectory("ksync-oauth").resolve("ksync").resolve("credentials.json");
        store = new CredentialStore(storePath);
    }

    @After
    public void tearDown() {
        server.stop();
    }

    private OAuth2Client client() {
        OAuth2Client c = new OAuth2Client(server.baseUrl(), store);
        // stand in for the user: fetch the authorize url and follow the redirect to the listener
        c.browserLauncher = url -> {
            try {
                HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
                con.setInstanceFollowRedirects(true);
                con.getInputStream().close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        };
        return c;
    }

    private CredentialStore.Credentials stored() throws IOException {
        CredentialStore.Credentials c = store.get(server.baseUrl());
        return c == null ? new CredentialStore.Credentials() : c;
    }

    private OAuth2Client noBrowserClient() {
        OAuth2Client c = client();
        c.browserLauncher = url -> {
            throw new AssertionError("should not have needed a browser, url=" + url);
        };
        return c;
    }

    private io.milton.httpclient.Host hostFor(OAuth2Client oauth) {
        return new BearerHost("127.0.0.1", "/", server.port(), "bob", oauth::accessToken, 10000);
    }

    @Test
    public void loginRegistersAClientAndStoresTokens() throws Exception {
        OAuth2Client oauth = client();
        assertFalse(oauth.hasSession());

        oauth.login();

        assertNotNull(stored().clientId);
        assertTrue(stored().accessToken.startsWith("ko2-"));
        assertNotNull(stored().refreshToken);
        assertTrue(oauth.hasSession());
        // registered as a public client, so no secret is stored anywhere
        assertEquals("none", server.registeredAuthMethod);
        assertNull("a public client must not be issued a secret", stored().clientSecret);
        assertTrue(server.pkceVerified);
    }

    @Test
    public void tokenAuthenticatesGetAndPut() throws Exception {
        OAuth2Client oauth = client();
        oauth.login();

        io.milton.httpclient.Host host = hostFor(oauth);
        byte[] body = host.doGet(Path.path("/branch/file.txt"), new HashMap<String, String>());
        assertEquals("authorized", new String(body, StandardCharsets.UTF_8));
        assertEquals("Bearer " + stored().accessToken, server.lastAuthHeader);

        host.doPut(Path.path("/branch/site.css"), "body{}".getBytes(StandardCharsets.UTF_8), null);
        assertEquals("PUT", server.lastProtectedMethod);
        assertEquals("Bearer " + stored().accessToken, server.lastAuthHeader);
    }

    @Test
    public void aLaterRunReusesTheStoredSessionWithoutABrowser() throws Exception {
        client().login();

        // a fresh OAuth2Client, as a separate ksync invocation would build
        OAuth2Client reloaded = noBrowserClient();
        assertTrue(reloaded.hasSession());
        assertEquals("authorized", new String(hostFor(reloaded).doGet(Path.path("/branch/f"), new HashMap<String, String>()), StandardCharsets.UTF_8));
    }

    @Test
    public void anExpiredTokenIsRefreshedWithoutABrowser() throws Exception {
        client().login();
        String originalAccess = stored().accessToken;
        String originalRefresh = stored().refreshToken;

        expire(0); // pretend an hour went by

        OAuth2Client oauth = noBrowserClient();
        assertEquals("authorized", new String(hostFor(oauth).doGet(Path.path("/branch/f"), new HashMap<String, String>()), StandardCharsets.UTF_8));

        assertFalse("access token should have been replaced", originalAccess.equals(stored().accessToken));
        assertFalse("refresh tokens rotate, so it must be replaced too", originalRefresh.equals(stored().refreshToken));
        assertTrue(stored().expiresAt > System.currentTimeMillis());
    }

    /** The refresh has to happen before the token dies, not after a request has already failed. */
    @Test
    public void tokenIsRefreshedPreemptivelyWithinTheLeeway() throws Exception {
        client().login();
        String originalAccess = stored().accessToken;

        // still valid, but expires in 10s - inside the refresh leeway
        expire(System.currentTimeMillis() + 10_000);

        assertFalse(originalAccess.equals(noBrowserClient().accessToken()));
    }

    @Test
    public void logoutClearsTokensButKeepsTheRegistration() throws Exception {
        OAuth2Client oauth = client();
        oauth.login();
        String clientId = stored().clientId;

        oauth.logout();

        assertNull(stored().accessToken);
        assertNull(stored().refreshToken);
        assertEquals("re-login should not need to register again", clientId, stored().clientId);
        assertFalse(oauth.hasSession());
    }

    @Test
    public void expiredSessionWithNoRefreshTokenTellsTheUserToLogIn() throws Exception {
        client().login();
        CredentialStore.Credentials c = stored();
        c.refreshToken = null;
        c.expiresAt = 0;
        store.put(server.baseUrl(), c);

        try {
            noBrowserClient().accessToken();
            org.junit.Assert.fail("expected an error telling the user to log in");
        } catch (NotLoggedInException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("login"));
        }
    }

    @Test
    public void aDeniedAuthorizationIsReported() throws Exception {
        server.denyAuthorization = true;
        try {
            client().login();
            org.junit.Assert.fail("expected the denial to surface");
        } catch (IOException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("access_denied"));
        }
    }

    /** A mismatched state means the code did not come from the request we started. */
    @Test
    public void aMismatchedStateIsRejected() throws Exception {
        server.tamperWithState = true;
        try {
            client().login();
            org.junit.Assert.fail("expected the bad state to be rejected");
        } catch (IOException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("state"));
        }
    }

    /** If our PKCE encoding drifted from the server's the exchange must fail loudly. */
    @Test
    public void aBadCodeVerifierIsRejectedByTheServer() throws Exception {
        server.corruptChallenge = true;
        try {
            client().login();
            org.junit.Assert.fail("expected PKCE verification to fail");
        } catch (IOException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("pkce"));
        }
    }


    // ---------------------------------------------------------------------------------

    private static class FakeAuthServer {

        private final HttpServer http;
        private final Map<String, String> clients = new ConcurrentHashMap<>();      // clientId -> redirectUri
        private final Map<String, String[]> codes = new ConcurrentHashMap<>();      // code -> {challenge, redirectUri}
        private final Map<String, String> refreshTokens = new ConcurrentHashMap<>();// refresh -> access
        String registeredAuthMethod;
        int registrationCalls;
        String lastAuthHeader;
        String lastProtectedMethod;
        boolean pkceVerified;
        boolean denyAuthorization;
        boolean tamperWithState;
        boolean corruptChallenge;

        FakeAuthServer() throws IOException {
            http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            http.createContext("/", this::route);
            http.start();
        }

        String preRegister(String redirectUri) {
            String id = UUID.randomUUID().toString();
            clients.put(id, redirectUri);
            return id;
        }

        String redirectUriFor(String clientId) {
            return clients.get(clientId);
        }

        int port() {
            return http.getAddress().getPort();
        }

        String baseUrl() {
            return "http://127.0.0.1:" + port();
        }

        void stop() {
            http.stop(0);
        }

        private void route(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/.well-known/oauth-authorization-server")) {
                metadata(ex);
            } else if (path.equals("/_oauth2/register")) {
                register(ex);
            } else if (path.equals("/_oauth2/authorize")) {
                authorize(ex);
            } else if (path.equals("/_oauth2/token")) {
                token(ex);
            } else {
                protectedResource(ex);
            }
        }

        private void metadata(HttpExchange ex) throws IOException {
            send(ex, 200, "{"
                    + "\"issuer\":\"" + baseUrl() + "/_oauth2\","
                    + "\"authorization_endpoint\":\"" + baseUrl() + "/_oauth2/authorize\","
                    + "\"token_endpoint\":\"" + baseUrl() + "/_oauth2/token\","
                    + "\"registration_endpoint\":\"" + baseUrl() + "/_oauth2/register\","
                    + "\"code_challenge_methods_supported\":[\"S256\"]}");
        }

        private void register(HttpExchange ex) throws IOException {
            net.sf.json.JSONObject body = net.sf.json.JSONObject.fromObject(read(ex));
            String redirectUri = body.getJSONArray("redirect_uris").getString(0);
            // the real server only allows https, localhost or 127.0.0.1
            String hostName = java.net.URI.create(redirectUri).getHost();
            if (!redirectUri.startsWith("https://") && !"localhost".equals(hostName) && !"127.0.0.1".equals(hostName)) {
                send(ex, 400, "{\"error\":\"invalid_redirect_uri\"}");
                return;
            }
            registrationCalls++;
            registeredAuthMethod = body.optString("token_endpoint_auth_method");
            String clientId = UUID.randomUUID().toString();
            clients.put(clientId, redirectUri);
            send(ex, 200, "{\"client_id\":\"" + clientId + "\",\"token_endpoint_auth_method\":\"" + registeredAuthMethod + "\"}");
        }

        private void authorize(HttpExchange ex) throws IOException {
            Map<String, String> q = OAuth2Client.parseQuery(ex.getRequestURI().getRawQuery());
            String redirectUri = clients.get(q.get("client_id"));
            if (redirectUri == null || !redirectUri.equals(q.get("redirect_uri"))) {
                send(ex, 400, "{\"error\":\"invalid_client\"}");
                return;
            }
            if (!"S256".equals(q.get("code_challenge_method")) || q.get("code_challenge") == null) {
                send(ex, 400, "{\"error\":\"invalid_request\"}");
                return;
            }
            String state = tamperWithState ? "not-the-state-you-sent" : q.get("state");
            String location;
            if (denyAuthorization) {
                location = redirectUri + "?error=access_denied&error_description=The+resource+owner+denied+the+request&state=" + state;
            } else {
                String code = UUID.randomUUID().toString();
                String challenge = corruptChallenge ? "a-challenge-that-cannot-match" : q.get("code_challenge");
                codes.put(code, new String[]{challenge, redirectUri});
                location = redirectUri + "?code=" + code + "&state=" + state;
            }
            ex.getResponseHeaders().add("Location", location);
            ex.sendResponseHeaders(302, -1);
            ex.close();
        }

        private void token(HttpExchange ex) throws IOException {
            Map<String, String> form = OAuth2Client.parseQuery(read(ex));
            if (!clients.containsKey(form.get("client_id"))) {
                send(ex, 401, "{\"error\":\"invalid_client\"}");
                return;
            }
            String grant = form.get("grant_type");
            if ("authorization_code".equals(grant)) {
                String[] rec = codes.remove(form.get("code"));
                if (rec == null) {
                    send(ex, 401, "{\"error\":\"invalid_grant\"}");
                    return;
                }
                if (!rec[1].equals(form.get("redirect_uri"))) {
                    send(ex, 400, "{\"error\":\"invalid_grant\",\"error_description\":\"redirect_uri mismatch\"}");
                    return;
                }
                if (!s256(form.get("code_verifier")).equals(rec[0])) {
                    send(ex, 400, "{\"error\":\"invalid_grant\",\"error_description\":\"PKCE verification failed\"}");
                    return;
                }
                pkceVerified = true;
            } else if ("refresh_token".equals(grant)) {
                // the real server deletes the old record, so refresh tokens are single use
                if (refreshTokens.remove(form.get("refresh_token")) == null) {
                    send(ex, 400, "{\"error\":\"invalid_grant\"}");
                    return;
                }
            } else {
                send(ex, 400, "{\"error\":\"unsupported_grant_type\"}");
                return;
            }
            String access = "ko2-" + UUID.randomUUID();
            String refresh = UUID.randomUUID().toString();
            refreshTokens.put(refresh, access);
            send(ex, 200, "{\"access_token\":\"" + access + "\",\"token_type\":\"Bearer\","
                    + "\"expires_in\":3600,\"refresh_token\":\"" + refresh + "\",\"scope\":\"profile\"}");
        }

        private void protectedResource(HttpExchange ex) throws IOException {
            drain(ex);
            lastAuthHeader = ex.getRequestHeaders().getFirst("Authorization");
            lastProtectedMethod = ex.getRequestMethod();
            if (lastAuthHeader == null || !lastAuthHeader.startsWith("Bearer ko2-")) {
                send(ex, 401, "unauthorized");
                return;
            }
            if (!refreshTokens.containsValue(lastAuthHeader.substring("Bearer ".length()))) {
                send(ex, 401, "stale token");
                return;
            }
            send(ex, 200, "authorized");
        }

        private static String s256(String verifier) {
            try {
                byte[] d = MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII));
                return Base64.getUrlEncoder().withoutPadding().encodeToString(d);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private static String read(HttpExchange ex) throws IOException {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try (InputStream in = ex.getRequestBody()) {
                byte[] buf = new byte[4096];
                int n;
                while ((n = in.read(buf)) > 0) {
                    bout.write(buf, 0, n);
                }
            }
            return new String(bout.toByteArray(), StandardCharsets.UTF_8);
        }

        private static void drain(HttpExchange ex) throws IOException {
            read(ex);
        }

        private static void send(HttpExchange ex, int status, String body) throws IOException {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", body.startsWith("{") ? "application/json" : "text/plain");
            ex.sendResponseHeaders(status, b.length);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(b);
            }
        }
    }
}
