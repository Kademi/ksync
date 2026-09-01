package co.kademi.sync;

import io.milton.httpclient.Host;
import java.util.function.Supplier;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.Credentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

/**
 * A milton Host which authenticates with "Authorization: Bearer {token}" instead of Basic auth
 * or cookies, for the KOAuth2 app on the server.
 *
 * The token is supplied per-request rather than fixed at construction, because OAuth2 access
 * tokens last an hour and a background sync runs for longer than that. The supplier is expected
 * to hand back a fresh token, refreshing if it is close to expiry.
 */
public class BearerHost extends Host {

    private final Supplier<String> tokenSupplier;

    /**
     * @param user only used to make Host register its auth interceptor, and for display; the
     * credentials themselves are never sent. May not be null.
     */
    public BearerHost(String server, String rootPath, Integer port, String user, Supplier<String> tokenSupplier, int timeoutMillis) {
        super(server, rootPath, port, user == null ? "oauth2" : user, "", null, timeoutMillis,
                new java.util.concurrent.ConcurrentHashMap<>(), null);
        this.tokenSupplier = tokenSupplier;
        setUsePreemptiveAuth(true);
        setUseDigestForPreemptiveAuth(false);
    }

    @Override
    protected HttpContext newContext() {
        HttpContext context = super.newContext();
        // ponytail: Host.PreemptiveAuthInterceptor only honours a scheme which is an instanceof
        // BasicScheme or DigestScheme, so a plain AuthScheme would be silently ignored. Subclassing
        // BasicScheme is the only way to inject a Bearer header without patching milton-client.
        // If milton-client ever grows a setAuthScheme(), use that instead and delete BearerScheme.
        context.setAttribute("preemptive-auth", new BearerScheme(tokenSupplier.get()));
        return context;
    }

    private static class BearerScheme extends BasicScheme {

        private final String token;

        BearerScheme(String token) {
            this.token = token;
        }

        @Override
        public String getSchemeName() {
            return "bearer";
        }

        @Override
        public Header authenticate(Credentials credentials, HttpRequest request, HttpContext context) {
            return authenticate(credentials, request);
        }

        @Override
        public Header authenticate(Credentials credentials, HttpRequest request) {
            return new BasicHeader(AUTH.WWW_AUTH_RESP, "Bearer " + token);
        }
    }
}
