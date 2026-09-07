package co.kademi.sync;

import java.io.File;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Surefire runs without a terminal, so System.console() is null here and every case below takes
 * the branch that must not prompt. That is the branch that matters: a background sync or a cron
 * job has nobody to answer, and blocking there is indistinguishable from a hang.
 */
public class RequireUrlTest {

    private final File dir = new File("/some/checkout");

    private SetupException requireUrlFails(String url) {
        try {
            KSyncUtils.requireUrl(url, dir, null, null);
            fail("Expected a SetupException for url: " + url);
            return null;
        } catch (SetupException ex) {
            return ex;
        }
    }

    @Test
    public void aUrl_isKept() {
        assertEquals("https://acme.kademi.com/repos/theme/version1",
                KSyncUtils.requireUrl("https://acme.kademi.com/repos/theme/version1", dir, null, null));
    }

    @Test
    public void surroundingSpace_isTrimmed() {
        // a url pasted into a terminal or a properties file often brings some with it
        assertEquals("https://acme.kademi.com/repos/theme",
                KSyncUtils.requireUrl("  https://acme.kademi.com/repos/theme\t", dir, null, null));
    }

    @Test
    public void noUrl_andNoTerminalToAskAt_saysWhatToDo() {
        String msg = requireUrlFails(null).getMessage();
        assertTrue(msg, msg.contains(dir.getAbsolutePath()));
        assertTrue(msg, msg.contains("not a ksync checkout"));
        assertTrue(msg, msg.contains("-url"));
    }

    @Test
    public void blankUrl_isTreatedAsNoUrl() {
        // an empty url= line in ksync.properties, which is not the same as the key being absent
        assertTrue(requireUrlFails("   ").getMessage().contains("not a ksync checkout"));
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
