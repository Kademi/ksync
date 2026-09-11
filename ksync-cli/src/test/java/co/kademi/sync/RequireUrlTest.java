package co.kademi.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Whether a url was given at all is settled by picocli, which marks it required and fills it from
 * the checkout's properties file. What is left here is the shape of it, which used to surface as a
 * MalformedURLException from deep inside the KSync3 constructor.
 */
public class RequireUrlTest {

    private SetupException requireUrlFails(String url) {
        try {
            KSyncUtils.requireUrl(url);
            fail("Expected a SetupException for url: " + url);
            return null;
        } catch (SetupException ex) {
            return ex;
        }
    }

    @Test
    public void aUrl_isKept() {
        assertEquals("https://acme.kademi.com/repos/theme/version1",
                KSyncUtils.requireUrl("https://acme.kademi.com/repos/theme/version1"));
    }

    @Test
    public void surroundingSpace_isTrimmed() {
        // a url pasted into a terminal or a properties file often brings some with it
        assertEquals("https://acme.kademi.com/repos/theme",
                KSyncUtils.requireUrl("  https://acme.kademi.com/repos/theme\t"));
    }

    @Test
    public void blankUrl_isTreatedAsNoUrl() {
        // an empty url= line in ksync.properties, which is not the same as the key being absent
        assertTrue(requireUrlFails("   ").getMessage().contains("nothing to sync with"));
        assertTrue(requireUrlFails(null).getMessage().contains("nothing to sync with"));
    }

    @Test
    public void notAUrl_saysSo_ratherThanThrowingFromTheConstructor() {
        String msg = requireUrlFails("content-lib").getMessage();
        assertTrue(msg, msg.contains("Not a valid url: content-lib"));
    }

    @Test
    public void aPathWithNoScheme_isNotAUrl() {
        assertTrue(requireUrlFails("acme.kademi.com/repos/theme").getMessage().contains("Not a valid url"));
    }
}
