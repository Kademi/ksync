package co.kademi.sync.oauth;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * The protocol itself is Nimbus' job now, so what is left to pin here is the loopback listener
 * and the small helpers around it.
 */
public class OAuth2ClientTest {

    @Test
    public void parsesQueryIncludingEncodedValues() {
        Map<String, String> p = OAuth2Client.parseQuery("code=abc123&state=xy_z&error_description=Not+allowed%3A+nope");
        assertEquals("abc123", p.get("code"));
        assertEquals("xy_z", p.get("state"));
        assertEquals("Not allowed: nope", p.get("error_description"));
    }

    @Test
    public void parsesEmptyAndMissingQuery() {
        assertTrue(OAuth2Client.parseQuery(null).isEmpty());
        assertTrue(OAuth2Client.parseQuery("").isEmpty());
        assertEquals("", OAuth2Client.parseQuery("code").get("code"));
    }

    @Test
    public void portOfHandlesJunk() {
        assertEquals(0, OAuth2Client.portOf(null));
        assertEquals(0, OAuth2Client.portOf(""));
        assertEquals(0, OAuth2Client.portOf("not a uri"));
        assertEquals(0, OAuth2Client.portOf("http://127.0.0.1/callback"));
        assertEquals(54321, OAuth2Client.portOf("http://127.0.0.1:54321/callback"));
    }

    /**
     * The listener must hand back the full callback uri, query included, because that is what
     * AuthorizationResponse.parse consumes.
     */
    @Test
    public void callbackReturnsTheWholeRedirectUri() throws Exception {
        try (OAuth2Client.Callback cb = new OAuth2Client.Callback(0)) {
            String redirect = cb.redirectUri();
            assertTrue(redirect.startsWith("http://127.0.0.1:"));
            assertTrue(redirect.endsWith("/callback"));

            new Thread(() -> get(redirect + "?code=THECODE&state=THESTATE")).start();

            URI got = cb.await();
            Map<String, String> params = OAuth2Client.parseQuery(got.getRawQuery());
            assertEquals("THECODE", params.get("code"));
            assertEquals("THESTATE", params.get("state"));
            assertEquals(redirect, got.getScheme() + "://" + got.getAuthority() + got.getPath());
        }
    }

    @Test
    public void callbackReceivesADenial() throws Exception {
        try (OAuth2Client.Callback cb = new OAuth2Client.Callback(0)) {
            new Thread(() -> get(cb.redirectUri() + "?error=access_denied&error_description=Denied")).start();
            Map<String, String> params = OAuth2Client.parseQuery(cb.await().getRawQuery());
            assertEquals("access_denied", params.get("error"));
        }
    }

    /** A second run must be able to reclaim the registered port, or the client_id is useless. */
    @Test
    public void callbackReusesItsPortAcrossRuns() throws Exception {
        int port;
        try (OAuth2Client.Callback first = new OAuth2Client.Callback(0)) {
            port = OAuth2Client.portOf(first.redirectUri());
        }
        try (OAuth2Client.Callback second = new OAuth2Client.Callback(port)) {
            assertEquals(port, OAuth2Client.portOf(second.redirectUri()));
        }
    }

    /** If the remembered port is taken, fall back to a free one rather than failing outright. */
    @Test
    public void callbackFallsBackWhenItsPortIsTaken() throws Exception {
        try (OAuth2Client.Callback holder = new OAuth2Client.Callback(0)) {
            int taken = OAuth2Client.portOf(holder.redirectUri());
            try (OAuth2Client.Callback second = new OAuth2Client.Callback(taken)) {
                assertNotNull(second.redirectUri());
                assertTrue(OAuth2Client.portOf(second.redirectUri()) != taken);
            }
        }
    }

    private static void get(String url) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setRequestMethod("GET");
            c.getInputStream().close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
