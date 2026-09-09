package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A password typed at a prompt has to end up as a stored session, or every sync and every pull
 * asks for it again. That hinges on finding one cookie in the login response, and servers return
 * cookies in more than one shape.
 */
public class SavedLoginTest {

    private static final String URL = "https://acme.kademi.com/repo/branch";

    private Path storePath;

    @Before
    public void setUp() throws Exception {
        // a throwaway store, so the test cannot read or write the developer's real credentials
        storePath = Files.createTempDirectory("ksync-saved-login").resolve("credentials.json");
        System.setProperty(CredentialStore.PATH_PROPERTY, storePath.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(CredentialStore.PATH_PROPERTY);
    }

    private static Map<String, String> setCookie(String value) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Set-Cookie", value);
        return headers;
    }

    /** One cookie per header, which the http client joins into one value */
    @Test
    public void aCookiePerHeader_isRead() {
        Map<String, String> headers = setCookie("miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/; Expires=Wed, 04-Sep-2019 23:59:47 GMT, "
                + "miltonUserUrlHash=\"YYY-XXX:DDDD\"; Path=/; Expires=Sat, 24-Aug-2019 02:29:54 GMT; HttpOnly");

        assertEquals("YYY-XXX:DDDD", KSync3.userUrlHashFrom(headers));
    }

    /**
     * The hash cookie first in the line. Worth its own case: the old code found the cookie by
     * index and read a position of 0 as "not found", so exactly this response saved nothing and
     * the password was asked for again on the next run.
     */
    @Test
    public void theHashCookieFirst_isRead() {
        Map<String, String> headers = setCookie("miltonUserUrlHash=\"AAA-BBB:CCC\"; Path=/; HttpOnly, "
                + "miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/");

        assertEquals("AAA-BBB:CCC", KSync3.userUrlHashFrom(headers));
    }

    /** A login that returns 200 but no auth cookie leaves nothing to save, and must not throw */
    @Test
    public void aResponseWithoutTheCookie_isNothingToSave() {
        assertNull(KSync3.userUrlHashFrom(null));
        assertNull(KSync3.userUrlHashFrom(new HashMap<>()));
        assertNull(KSync3.userUrlHashFrom(setCookie("")));
        assertNull(KSync3.userUrlHashFrom(setCookie("JSESSIONID=abc; Path=/")));
        assertNull(KSync3.userUrlHashFrom(setCookie("miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/")));
    }

    /** Truncated by a proxy, or a server that quotes differently. Better than half a hash */
    @Test
    public void anUnterminatedCookie_isNothingToSave() {
        assertNull(KSync3.userUrlHashFrom(setCookie("miltonUserUrlHash=\"YYY-XXX:DDDD; Path=/")));
    }

    /**
     * What the saving is for: the next run finds the cookies and so never reaches the password
     * prompt. Both are needed - milton authenticates on the pair.
     */
    @Test
    public void aSavedLogin_isWhatTheNextRunFinds() {
        KSyncUtils.writeLoginProps("/users/brad/", "YYY-XXX:DDDD", URL);

        Map<String, String> cookies = KSyncUtils.getCookies(URL);
        assertEquals("/users/brad/", cookies.get("miltonUserUrl"));
        assertEquals("YYY-XXX:DDDD", cookies.get("miltonUserUrlHash"));
    }

    /** Stored per site, so a login for one server is not offered to another */
    @Test
    public void aSavedLogin_belongsToItsSite() {
        KSyncUtils.writeLoginProps("/users/brad/", "YYY-XXX:DDDD", URL);

        assertEquals(true, KSyncUtils.getCookies("https://other.kademi.com/repo/branch").isEmpty());
    }
}
