package co.kademi.sync.oauth;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationErrorResponse;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.AuthorizationResponse;
import com.nimbusds.oauth2.sdk.AuthorizationSuccessResponse;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerMetadata;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.client.ClientInformationResponse;
import com.nimbusds.oauth2.sdk.client.ClientMetadata;
import com.nimbusds.oauth2.sdk.client.ClientRegistrationRequest;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.sun.net.httpserver.HttpServer;
import java.awt.Desktop;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2.1 authorization code flow with PKCE against the KOAuth2 app on the server, using the
 * Nimbus oauth2-oidc-sdk for the protocol itself.
 *
 * The initial login is interactive: it needs a browser, because the server only offers the
 * authorization_code and refresh_token grants, so there is no headless way to mint a token.
 * After that the tokens live in the repo's ksync.properties and {@link #accessToken()} keeps
 * them fresh with no further user interaction.
 *
 * A client id can either be registered dynamically (RFC7591), which is what happens by default,
 * or configured up front with -clientid for servers where dynamic registration is turned off.
 */
public class OAuth2Client {

    private static final Logger log = LoggerFactory.getLogger(OAuth2Client.class);

    private static final String METADATA_PATH = "/.well-known/oauth-authorization-server";
    private static final Scope SCOPE = new Scope("profile");
    // The server gives an auth code 30 seconds to live, so there is no point waiting much
    // longer than a few minutes for the user to finish in the browser.
    private static final int BROWSER_TIMEOUT_SECS = 300;
    // Refresh this far ahead of real expiry, so a request never goes out holding a token which
    // expires while it is in flight.
    private static final long REFRESH_LEEWAY_MILLIS = 60_000;

    /**
     * Checked before the stored credentials, so CI can supply a KOAuth2 api key (ko2_ak_...)
     * without a login and without writing anything to disk. Used exactly as given: no refresh,
     * and nothing persisted. Matches ksync-go's KSYNC_TOKEN.
     */
    public static final String TOKEN_ENV_VAR = "KSYNC_TOKEN";

    /** Replaced by tests to stand in for the user and their browser. */
    java.util.function.Consumer<String> browserLauncher = OAuth2Client::openBrowser;

    /** Replaced by tests, which cannot set a real environment variable on a modern jvm. */
    java.util.function.Supplier<String> envTokenSource = () -> System.getenv(TOKEN_ENV_VAR);

    private final String baseUrl;
    private final String host;
    private final CredentialStore store;
    private CredentialStore.Credentials creds;

    public OAuth2Client(String baseUrl, CredentialStore store) {
        this.baseUrl = StringUtils.removeEnd(baseUrl, "/");
        this.host = CredentialStore.normalizeHost(baseUrl);
        this.store = store;
    }

    private CredentialStore.Credentials creds() {
        if (creds == null) {
            try {
                creds = store.get(host);
            } catch (IOException ex) {
                throw new RuntimeException("Could not read " + store.getPath(), ex);
            }
            if (creds == null) {
                creds = new CredentialStore.Credentials();
            }
        }
        return creds;
    }

    private void saveCreds() throws IOException {
        store.put(host, creds());
    }

    /** An api key from the environment, or null. */
    String envToken() {
        return StringUtils.trimToNull(envTokenSource.get());
    }

    /**
     * @return true if there is a stored session which can be used, or refreshed, without sending
     * the user back to a browser
     */
    public boolean hasSession() {
        if (envToken() != null) {
            return true;
        }
        return StringUtils.isNotBlank(creds().accessToken) || StringUtils.isNotBlank(creds().refreshToken);
    }

    /**
     * The current access token, refreshed first if it has expired or is about to. Suitable to
     * hand to {@link co.kademi.sync.BearerHost} as its token supplier.
     */
    public synchronized String accessToken() {
        String fromEnv = envToken();
        if (fromEnv != null) {
            // supplied explicitly, so use it as given: no refresh, nothing written to disk
            return fromEnv;
        }
        CredentialStore.Credentials c = creds();
        if (StringUtils.isBlank(c.accessToken) && StringUtils.isBlank(c.refreshToken)) {
            throw new NotLoggedInException("Not logged in to " + host + ". Run: ksync -command login -oauth"
                    + ", or set " + TOKEN_ENV_VAR + " to an api key");
        }
        if (System.currentTimeMillis() > c.expiresAt - REFRESH_LEEWAY_MILLIS) {
            if (StringUtils.isBlank(c.refreshToken)) {
                throw new NotLoggedInException("The token for " + host + " has expired and there is no refresh"
                        + " token. Run: ksync -command login -oauth");
            }
            log.debug("Access token expired or expiring, refreshing");
            refresh();
        }
        return creds().accessToken;
    }

    /**
     * Runs the interactive login: registers this ksync install as an OAuth2 client if it does not
     * have an id already, opens a browser for the user to approve, and exchanges the resulting
     * code for tokens. Blocks until the user finishes or {@value #BROWSER_TIMEOUT_SECS} seconds pass.
     */
    public void login() throws IOException {
        AuthorizationServerMetadata metadata = discover();
        log.debug("Authorization server: {}", metadata.getIssuer());

        // The redirect uri is bound to the client registration, so the callback listener has to
        // come up on the same port every time. Claim it now and hold it for the whole flow.
        try (Callback callback = new Callback(portOf(creds().redirectUri))) {
            URI callbackUri = URI.create(callback.redirectUri());
            if (StringUtils.isBlank(creds().clientId) || !callback.redirectUri().equals(creds().redirectUri)) {
                register(metadata, callbackUri);
            }

            CodeVerifier verifier = new CodeVerifier();
            State state = new State();
            URI authUri = new AuthorizationRequest.Builder(new ResponseType(ResponseType.Value.CODE), new ClientID(creds().clientId))
                    .endpointURI(metadata.getAuthorizationEndpointURI())
                    .redirectionURI(callbackUri)
                    .scope(SCOPE)
                    .state(state)
                    .codeChallenge(verifier, CodeChallengeMethod.S256)
                    .build()
                    .toURI();

            browserLauncher.accept(authUri.toString());

            AuthorizationResponse response;
            try {
                response = AuthorizationResponse.parse(callback.await());
            } catch (ParseException ex) {
                throw new IOException("Could not read the authorization response: " + ex.getMessage(), ex);
            }
            if (!response.indicatesSuccess()) {
                AuthorizationErrorResponse error = response.toErrorResponse();
                throw new IOException("Authorization was refused: " + error.getErrorObject().getCode()
                        + " - " + error.getErrorObject().getDescription());
            }
            AuthorizationSuccessResponse success = response.toSuccessResponse();
            if (!state.equals(success.getState())) {
                throw new IOException("Authorization response carried the wrong state, discarding it");
            }

            exchange(metadata.getTokenEndpointURI(), new AuthorizationCodeGrant(success.getAuthorizationCode(), callbackUri, verifier));
            log.info("Login complete");
        }
    }

    /** Discards the stored session. The client registration is kept, so a re-login skips it. */
    public void logout() throws IOException {
        CredentialStore.Credentials c = creds();
        c.accessToken = null;
        c.refreshToken = null;
        c.expiresAt = 0;
        c.scopes.clear();
        saveCreds();
    }

    // --- flow steps ----------------------------------------------------------------------

    /**
     * KOAuth2 serves its metadata from the website root while its issuer is {root}/_oauth2, so
     * the well known path cannot be derived from the issuer the way Nimbus' resolve() does it.
     */
    private AuthorizationServerMetadata discover() throws IOException {
        HTTPRequest request = new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + METADATA_PATH).toURL());
        HTTPResponse response = request.send();
        try {
            return AuthorizationServerMetadata.parse(response.getBodyAsJSONObject());
        } catch (ParseException ex) {
            throw new IOException("Could not read the authorization server metadata from "
                    + baseUrl + METADATA_PATH + ": " + ex.getMessage(), ex);
        }
    }

    private void register(AuthorizationServerMetadata metadata, URI callbackUri) throws IOException {
        URI endpoint = metadata.getRegistrationEndpointURI();
        if (endpoint == null) {
            throw new IOException("This server does not offer dynamic client registration, so a client id"
                    + " must be supplied with -clientid");
        }
        log.debug("Registering ksync as an OAuth2 client at {}", endpoint);

        ClientMetadata client = new ClientMetadata();
        client.setRedirectionURI(callbackUri);
        client.setGrantTypes(new java.util.HashSet<>(java.util.Arrays.asList(GrantType.AUTHORIZATION_CODE, GrantType.REFRESH_TOKEN)));
        client.setResponseTypes(Collections.singleton(new ResponseType(ResponseType.Value.CODE)));
        // public client: no secret to keep on a developer machine, and PKCE is then mandatory
        client.setTokenEndpointAuthMethod(ClientAuthenticationMethod.NONE);
        client.setName("ksync");

        HTTPResponse response = new ClientRegistrationRequest(endpoint, client, null).toHTTPRequest().send();
        try {
            ClientInformationResponse parsed = ClientInformationResponse.parse(response);
            creds().clientId = parsed.getClientInformation().getID().getValue();
            if (parsed.getClientInformation().getSecret() != null) {
                creds().clientSecret = parsed.getClientInformation().getSecret().getValue();
            }
        } catch (ParseException ex) {
            throw new IOException("Client registration failed: " + ex.getMessage(), ex);
        }

        creds().redirectUri = callbackUri.toString();
        saveCreds();
        log.debug("Registered with client_id={}", creds().clientId);
    }

    private void refresh() {
        try {
            // the token endpoint was recorded at login, so a refresh costs no discovery call
            String endpoint = creds().tokenEndpoint;
            URI tokenUri = StringUtils.isNotBlank(endpoint)
                    ? URI.create(endpoint)
                    : discover().getTokenEndpointURI();
            exchange(tokenUri, new RefreshTokenGrant(new RefreshToken(creds().refreshToken)));
        } catch (TokenErrorException ex) {
            if (!ex.meansCredentialsAreDead()) {
                // the server is unhappy for some other reason; keep the credentials, because
                // throwing them away over a 503 would log the user out for someone else's outage
                throw new RuntimeException("Renewing the access token for " + host + " failed with "
                        + ex.getMessage() + ", which may be temporary. The stored login has been kept.", ex);
            }
            // The server rejected the refresh token itself: it has expired, been revoked, or
            // was already spent. Only a new login fixes that, so drop the dead credentials -
            // leaving them would make hasSession() keep choosing OAuth and fail identically on
            // every later command, instead of falling back to a password prompt.
            log.warn("The stored session for {} is no longer valid ({}), discarding it", host, ex.code);
            discardSession();
            if ("invalid_client".equalsIgnoreCase(ex.code)) {
                // our registration is gone from the server too, so the next login must re-register
                creds().clientId = null;
                creds().redirectUri = null;
                try {
                    saveCreds();
                } catch (IOException ignored) {
                    log.warn("Could not clear the stale client registration for {}", host);
                }
            }
            throw new NotLoggedInException("The session for " + host + " has expired and could not be renewed ("
                    + ex.getMessage() + "). Run: ksync -command login -oauth", ex);
        } catch (IOException ex) {
            // Could not reach the server. The credentials may well be fine, so keep them and say
            // so, rather than sending the user off to log in again for a network blip.
            throw new RuntimeException("Could not reach " + host + " to renew the access token, so this"
                    + " may be temporary. The stored login has been kept; try again, and if it persists"
                    + " run: ksync -command login -oauth", ex);
        }
    }

    /** Forgets tokens the server will no longer accept, keeping the client registration. */
    private void discardSession() {
        try {
            CredentialStore.Credentials c = creds();
            c.accessToken = null;
            c.refreshToken = null;
            c.expiresAt = 0;
            c.scopes.clear();
            saveCreds();
        } catch (IOException ex) {
            log.warn("Could not clear the expired session for {}", host, ex);
        }
    }

    private void exchange(URI tokenEndpoint, AuthorizationGrant grant) throws IOException {
        // public client, so the client id goes in the form rather than an auth header
        TokenRequest request = new TokenRequest(tokenEndpoint, new ClientID(creds().clientId), grant, SCOPE);
        TokenResponse response;
        try {
            response = TokenResponse.parse(request.toHTTPRequest().send());
        } catch (ParseException ex) {
            throw new IOException("Could not read the token response: " + ex.getMessage(), ex);
        }
        if (!response.indicatesSuccess()) {
            com.nimbusds.oauth2.sdk.ErrorObject error = response.toErrorResponse().getErrorObject();
            throw new TokenErrorException(error.getCode(), error.getDescription(), error.getHTTPStatusCode());
        }

        Tokens tokens = response.toSuccessResponse().getTokens();
        CredentialStore.Credentials c = creds();
        c.accessToken = tokens.getAccessToken().getValue();
        // Refresh tokens rotate: the server deletes the old record on every exchange, so the new
        // one has to replace what we hold, or the next refresh fails.
        if (tokens.getRefreshToken() != null) {
            c.refreshToken = tokens.getRefreshToken().getValue();
        }
        long lifetime = tokens.getAccessToken().getLifetime();
        c.expiresAt = System.currentTimeMillis() + (lifetime > 0 ? lifetime : 3600) * 1000L;
        c.tokenEndpoint = tokenEndpoint.toString();
        if (tokens.getAccessToken().getScope() != null) {
            c.scopes = new java.util.ArrayList<>(tokens.getAccessToken().getScope().toStringList());
        }
        saveCreds();
    }

    /** An OAuth2 error response from the token endpoint, as opposed to a failure to reach it. */
    static class TokenErrorException extends IOException {

        final String code;
        final int status;

        TokenErrorException(String code, String description, int status) {
            super((code == null ? "HTTP " + status : code) + (description == null ? "" : " - " + description));
            this.code = code;
            this.status = status;
        }

        /**
         * Whether this says the credentials are finished, as opposed to the server having a bad
         * day. Only the grant and client errors mean that: a 5xx, or an error body we could not
         * read, must never cost someone their login.
         */
        boolean meansCredentialsAreDead() {
            return "invalid_grant".equalsIgnoreCase(code) || "invalid_client".equalsIgnoreCase(code);
        }
    }

    // --- the loopback listener -----------------------------------------------------------

    /**
     * A one shot local HTTP listener for the authorization redirect. The server only permits
     * https, localhost or 127.0.0.1 redirect uris, so loopback is the supported option for a
     * command line client.
     */
    static class Callback implements AutoCloseable {

        private final HttpServer server;
        private final ArrayBlockingQueue<URI> received = new ArrayBlockingQueue<>(1);

        Callback(int preferredPort) throws IOException {
            HttpServer s;
            try {
                s = HttpServer.create(new InetSocketAddress("127.0.0.1", preferredPort), 0);
            } catch (IOException ex) {
                // the remembered port is taken, so take any free one and re-register
                s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            }
            this.server = s;
            server.createContext("/callback", exchange -> {
                URI uri = URI.create(redirectUriFor(server) + queryOf(exchange.getRequestURI()));
                received.offer(uri);
                boolean ok = String.valueOf(uri.getRawQuery()).contains("code=");
                byte[] page = (ok
                        ? "<html><body><h3>ksync is now authorized.</h3><p>You can close this window.</p></body></html>"
                        : "<html><body><h3>Authorization failed.</h3><p>Return to your terminal.</p></body></html>")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
                exchange.sendResponseHeaders(200, page.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(page);
                }
            });
            server.start();
        }

        private static String queryOf(URI requestUri) {
            return requestUri.getRawQuery() == null ? "" : "?" + requestUri.getRawQuery();
        }

        private static String redirectUriFor(HttpServer s) {
            return "http://127.0.0.1:" + s.getAddress().getPort() + "/callback";
        }

        String redirectUri() {
            return redirectUriFor(server);
        }

        /** @return the full callback uri including its query, for AuthorizationResponse.parse */
        URI await() throws IOException {
            try {
                URI uri = received.poll(BROWSER_TIMEOUT_SECS, TimeUnit.SECONDS);
                if (uri == null) {
                    throw new IOException("Timed out after " + BROWSER_TIMEOUT_SECS + "s waiting for authorization in the browser");
                }
                return uri;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for authorization", ex);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    static Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (StringUtils.isBlank(query)) {
            return map;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                map.put(dec(pair), "");
            } else {
                map.put(dec(pair.substring(0, eq)), dec(pair.substring(eq + 1)));
            }
        }
        return map;
    }

    static int portOf(String uri) {
        if (StringUtils.isBlank(uri)) {
            return 0;
        }
        try {
            int port = URI.create(uri).getPort();
            return port < 0 ? 0 : port;
        } catch (IllegalArgumentException ex) {
            return 0;
        }
    }

    private static void openBrowser(String url) {
        System.out.println();
        System.out.println("Opening your browser to authorize ksync. If it does not open, paste this in yourself:");
        System.out.println("  " + url);
        System.out.println();
        try {
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(URI.create(url));
            }
        } catch (Exception ex) {
            log.debug("Could not open a browser, the user will have to paste the url", ex);
        }
    }

    private static String dec(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (java.io.UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
