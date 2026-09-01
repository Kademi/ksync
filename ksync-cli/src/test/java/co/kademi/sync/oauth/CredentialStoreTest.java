package co.kademi.sync.oauth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class CredentialStoreTest {

    private Path path;
    private CredentialStore store;

    @Before
    public void setUp() throws Exception {
        path = Files.createTempDirectory("credstore").resolve("ksync").resolve("credentials.json");
        store = new CredentialStore(path);
    }

    private static CredentialStore.Credentials creds(String access) {
        CredentialStore.Credentials c = new CredentialStore.Credentials();
        c.clientId = "client-1";
        c.accessToken = access;
        c.refreshToken = "refresh-1";
        c.expiresAt = 1893456000000L; // 2030-01-01T00:00:00Z
        c.tokenEndpoint = "https://acme.kademi.com/_oauth2/token";
        c.redirectUri = "http://127.0.0.1:5000/callback";
        c.scopes = Arrays.asList("profile");
        return c;
    }

    @Test
    public void roundTripsEveryField() throws Exception {
        store.put("https://acme.kademi.com/repo/branch", creds("ko2-abc"));

        CredentialStore.Credentials got = store.get("https://acme.kademi.com/repo/branch");
        assertEquals("client-1", got.clientId);
        assertEquals("ko2-abc", got.accessToken);
        assertEquals("refresh-1", got.refreshToken);
        assertEquals(1893456000000L, got.expiresAt);
        assertEquals("https://acme.kademi.com/_oauth2/token", got.tokenEndpoint);
        assertEquals("http://127.0.0.1:5000/callback", got.redirectUri);
        assertEquals(Arrays.asList("profile"), got.scopes);
    }

    @Test
    public void missingHostReturnsNull() throws Exception {
        assertNull(store.get("nothing.kademi.com"));
    }

    /** One file has to serve several accounts without them treading on each other. */
    @Test
    public void keepsHostsSeparate() throws Exception {
        store.put("https://acme.kademi.com", creds("ko2-acme"));
        store.put("https://other.kademi.com", creds("ko2-other"));

        assertEquals("ko2-acme", store.get("acme.kademi.com").accessToken);
        assertEquals("ko2-other", store.get("other.kademi.com").accessToken);
        assertEquals(2, store.hosts().size());
    }

    /** A url with or without a scheme, path or case difference must find the same entry. */
    @Test
    public void normalizesTheHostKey() throws Exception {
        store.put("https://ACME.kademi.com/repo/branch/", creds("ko2-abc"));

        assertNotNull(store.get("acme.kademi.com"));
        assertNotNull(store.get("http://acme.kademi.com"));
        assertNotNull(store.get("https://acme.kademi.com/somewhere/else"));
        assertNotNull(store.get("  Acme.Kademi.Com  "));
    }

    /** A dev instance on another port is a different site, not the same one. */
    @Test
    public void portIsPartOfTheKey() throws Exception {
        store.put("http://localhost:8080", creds("ko2-eight"));
        store.put("http://localhost:9090", creds("ko2-nine"));

        assertEquals("ko2-eight", store.get("http://localhost:8080").accessToken);
        assertEquals("ko2-nine", store.get("http://localhost:9090").accessToken);
    }

    @Test
    public void deleteForgetsOnlyThatHost() throws Exception {
        store.put("acme.kademi.com", creds("ko2-acme"));
        store.put("other.kademi.com", creds("ko2-other"));

        store.delete("acme.kademi.com");

        assertNull(store.get("acme.kademi.com"));
        assertNotNull(store.get("other.kademi.com"));
    }

    /** The token is readable by anything that can read the file, so the mode is the protection. */
    @Test
    public void fileAndDirectoryAreOwnerOnly() throws Exception {
        Assume.assumeTrue(Files.getFileStore(path.getParent().getParent()).supportsFileAttributeView("posix"));
        store.put("acme.kademi.com", creds("ko2-abc"));

        assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(path)));
        assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(path.getParent())));
    }

    /**
     * Losing this file logs the user out and loses their refresh token, so a file we cannot
     * parse must stop us rather than be silently replaced.
     */
    @Test
    public void refusesToOverwriteAFileItCannotParse() throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, "this is not json".getBytes(StandardCharsets.UTF_8));

        try {
            store.put("acme.kademi.com", creds("ko2-abc"));
            fail("expected the store to refuse");
        } catch (IOException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("refusing to overwrite"));
        }
        assertEquals("this is not json", new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    @Test
    public void survivesAnEmptyFile() throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[0]);
        assertNull(store.get("acme.kademi.com"));
    }

    /**
     * The file is shared with ksync-go, so its output has to be readable here: RFC3339 expiry,
     * snake_case keys, and an empty access_token after a logout rather than an absent one.
     */
    @Test
    public void readsWhatKsyncGoWrites() throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, ("{\n"
                + "  \"acme.kademi.com\": {\n"
                + "    \"client_id\": \"go-client\",\n"
                + "    \"access_token\": \"ko2-from-go\",\n"
                + "    \"refresh_token\": \"go-refresh\",\n"
                + "    \"expires_at\": \"2030-01-01T00:00:00Z\",\n"
                + "    \"scopes\": [\"profile\"],\n"
                + "    \"token_endpoint\": \"https://acme.kademi.com/_oauth2/token\"\n"
                + "  },\n"
                + "  \"loggedout.kademi.com\": {\n"
                + "    \"client_id\": \"go-client-2\",\n"
                + "    \"access_token\": \"\"\n"
                + "  }\n"
                + "}\n").getBytes(StandardCharsets.UTF_8));

        CredentialStore.Credentials c = store.get("acme.kademi.com");
        assertEquals("go-client", c.clientId);
        assertEquals("ko2-from-go", c.accessToken);
        assertEquals(1893456000000L, c.expiresAt);
        assertEquals(Arrays.asList("profile"), c.scopes);

        // an empty access_token is a logged out entry, not a session
        CredentialStore.Credentials out = store.get("loggedout.kademi.com");
        assertNull(out.accessToken);
        assertNull(out.refreshToken);
    }

    /**
     * A real ksync-go file writes the expiry with a numeric offset and nanosecond precision,
     * not a trailing Z. Taken verbatim from one it produced.
     */
    @Test
    public void readsAnRfc3339ExpiryWithANumericOffset() throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, ("{\"rootorg.admin.loopbackdns.com:8080\": {"
                + "\"client_id\": \"c\", \"access_token\": \"ko2-abc\","
                + "\"expires_at\": \"2026-08-29T13:20:45.186642997+12:00\"}}").getBytes(StandardCharsets.UTF_8));

        CredentialStore.Credentials c = store.get("rootorg.admin.loopbackdns.com:8080");
        assertEquals("ko2-abc", c.accessToken);
        assertEquals(java.time.OffsetDateTime.parse("2026-08-29T13:20:45.186642997+12:00").toInstant().toEpochMilli(),
                c.expiresAt);
    }

    /** And our output has to be readable there: expiry back out as RFC3339. */
    @Test
    public void writesAnRfc3339Expiry() throws Exception {
        store.put("acme.kademi.com", creds("ko2-abc"));
        String text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        assertTrue(text, text.contains("\"expires_at\""));
        assertTrue(text, text.contains("2030-01-01T00:00:00Z"));
        assertTrue(text, text.contains("\"client_id\""));
        assertFalse("a public client has no secret to write", text.contains("client_secret"));
    }

    @Test
    public void writeIsAtomicallyReplacedLeavingNoTempFiles() throws Exception {
        store.put("acme.kademi.com", creds("ko2-one"));
        store.put("acme.kademi.com", creds("ko2-two"));

        assertEquals("ko2-two", store.get("acme.kademi.com").accessToken);
        try (java.util.stream.Stream<Path> files = Files.list(path.getParent())) {
            assertEquals(Arrays.asList(path.getFileName().toString()),
                    files.map(p -> p.getFileName().toString()).sorted().collect(java.util.stream.Collectors.toList()));
        }
    }

    /** Tokens must not land in the checkout, where a git commit could pick them up. */
    @Test
    public void defaultStoreLivesUnderTheUserConfigDirectory() {
        Path p = CredentialStore.defaultStore().getPath();
        assertEquals("credentials.json", p.getFileName().toString());
        assertEquals("ksync", p.getParent().getFileName().toString());
        assertTrue(p.toString(), p.startsWith(CredentialStore.userConfigDir()));
    }
}
