package co.kademi.sync.status;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class NotifierTest {

    /**
     * The body is a file path or an error message and goes into an AppleScript string. A quote in
     * it would end the string early and leave the rest to be run as script.
     */
    @Test
    public void appleScriptStringsAreEscaped() {
        assertEquals("\"plain\"", Notifier.asAppleScriptString("plain"));
        assertEquals("\"a \\\"quoted\\\" name\"", Notifier.asAppleScriptString("a \"quoted\" name"));
        assertEquals("\"back\\\\slash\"", Notifier.asAppleScriptString("back\\slash"));
        assertEquals("\"\"", Notifier.asAppleScriptString(null));
    }
}
