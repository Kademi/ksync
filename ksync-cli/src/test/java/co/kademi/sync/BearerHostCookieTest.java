package co.kademi.sync;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.milton.common.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Bearer auth must be the only credential ksync presents. These pin that no Cookie header ever
 * goes out, even when the server tries hard to set one.
 */
public class BearerHostCookieTest {

    private HttpServer server;
    private final List<String> cookieHeadersSeen = Collections.synchronizedList(new ArrayList<>());
    private final List<String> authHeadersSeen = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", (HttpExchange ex) -> {
            try (InputStream in = ex.getRequestBody()) {
                while (in.read() >= 0) {
                }
            }
            cookieHeadersSeen.add(String.valueOf(ex.getRequestHeaders().getFirst("Cookie")));
            authHeadersSeen.add(String.valueOf(ex.getRequestHeaders().getFirst("Authorization")));
            // try every way we can to get a cookie to stick
            ex.getResponseHeaders().add("Set-Cookie", "miltonUserUrl=/users/bob/; Path=/");
            ex.getResponseHeaders().add("Set-Cookie", "JSESSIONID=abc123; Path=/; HttpOnly");
            byte[] b = "ok".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
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

    private io.milton.httpclient.Host bearerHost() {
        return new BearerHost("127.0.0.1", "/", server.getAddress().getPort(), "bob", () -> "ko2-thetoken", 10000);
    }

    @Test
    public void neverSendsACookieHeaderEvenAfterTheServerSetsOne() throws Exception {
        io.milton.httpclient.Host host = bearerHost();
        for (int i = 0; i < 4; i++) {
            host.doGet(Path.path("/branch/f" + i), new HashMap<String, String>());
        }
        host.doPut(Path.path("/branch/site.css"), "body{}".getBytes(StandardCharsets.UTF_8), null);

        assertEquals(5, cookieHeadersSeen.size());
        for (String seen : cookieHeadersSeen) {
            assertEquals("a Cookie header was sent: " + seen, "null", seen);
        }
    }

    @Test
    public void everyRequestCarriesTheBearerTokenAndNothingElse() throws Exception {
        io.milton.httpclient.Host host = bearerHost();
        host.doGet(Path.path("/branch/f"), new HashMap<String, String>());
        host.doGet(Path.path("/branch/g"), new HashMap<String, String>());

        for (String auth : authHeadersSeen) {
            assertEquals("Bearer ko2-thetoken", auth);
        }
        assertTrue("no Basic auth may be sent", authHeadersSeen.stream().noneMatch(h -> h.startsWith("Basic")));
    }

    /** The supplier is consulted per request, so a refresh mid-session takes effect immediately. */
    @Test
    public void picksUpARefreshedTokenWithoutRebuildingTheHost() throws Exception {
        final String[] token = {"ko2-first"};
        io.milton.httpclient.Host host = new BearerHost("127.0.0.1", "/", server.getAddress().getPort(), "bob", () -> token[0], 10000);

        host.doGet(Path.path("/branch/f"), new HashMap<String, String>());
        token[0] = "ko2-second";
        host.doGet(Path.path("/branch/g"), new HashMap<String, String>());

        assertEquals("Bearer ko2-first", authHeadersSeen.get(0));
        assertEquals("Bearer ko2-second", authHeadersSeen.get(1));
    }
}
