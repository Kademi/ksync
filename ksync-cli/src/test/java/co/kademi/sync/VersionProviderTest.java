package co.kademi.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class VersionProviderTest {

    @Test
    public void oneLine_namingTheToolAndTheVersion() throws Exception {
        String[] lines = new VersionProvider().getVersion();
        assertEquals(1, lines.length);
        assertTrue(lines[0], lines[0].startsWith("ksync3 version "));
    }

    /**
     * Surefire runs against classes, not the shaded jar, so there is no manifest here. The point is
     * that it says so rather than printing "ksync3 version null ()".
     */
    @Test
    public void withoutAManifest_itSaysSoAndOmitsTheDate() throws Exception {
        assertEquals("development build", VersionProvider.version());
        assertNull(VersionProvider.buildDate());
        assertEquals("ksync3 version development build", new VersionProvider().getVersion()[0]);
    }
}
