package io.milton.sync;

import io.milton.sync.ConflictResolvers.Mode;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * A dialog is the default, but it is the wrong answer whenever nobody can click it. The trap is
 * that "has a display" and "has a person" are different questions: an agent driving ksync on a
 * developer's desktop has DISPLAY set and still cannot press a button.
 */
public class ConflictResolversTest {

    private static Map<String, String> env(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String why(Map<String, String> env, boolean display) {
        return ConflictResolvers.whyNotGui(env::get, display);
    }

    @Test
    public void aPersonAtADesktopGetsTheDialog() {
        assertNull(why(env("DISPLAY", ":0", "TERM", "xterm-256color"), true));
    }

    /**
     * The case that matters most: a display is available, so a headless check alone would say
     * "show a dialog" and the agent would wait forever on a window it cannot press.
     */
    @Test
    public void anAgentWithADisplayStillGetsTheTerminal() {
        String reason = why(env("CLAUDECODE", "1", "DISPLAY", ":0", "TERM", "xterm-256color"), true);
        assertNotNull("an agent must not be given a dialog it cannot click", reason);
        assertTrue(reason, reason.contains("Claude Code"));
        assertTrue(ConflictResolvers.create(Mode.CONSOLE) instanceof ConsoleConflictResolver);
    }

    @Test
    public void ciGetsTheTerminal() {
        assertTrue(why(env("CI", "true", "DISPLAY", ":0"), true).contains("CI"));
        assertTrue(why(env("CI", "1"), true).contains("CI"));
    }

    /** Some tools set CI=false; that is not CI. */
    @Test
    public void ciSetToFalseIsNotCi() {
        assertNull(why(env("CI", "false", "DISPLAY", ":0"), true));
        assertNull(why(env("CI", "0", "DISPLAY", ":0"), true));
        assertNull(why(env("CI", "", "DISPLAY", ":0"), true));
    }

    @Test
    public void otherNonInteractiveSignals() {
        assertTrue(why(env("KSYNC_NON_INTERACTIVE", "1", "DISPLAY", ":0"), true).contains("KSYNC_NON_INTERACTIVE"));
        assertTrue(why(env("DEBIAN_FRONTEND", "noninteractive", "DISPLAY", ":0"), true).contains("DEBIAN_FRONTEND"));
        assertTrue(why(env("TERM", "dumb", "DISPLAY", ":0"), true).contains("TERM"));
    }

    @Test
    public void noDisplayGetsTheTerminal() {
        assertTrue(why(env("TERM", "xterm"), false).contains("no display"));
    }

    @Test
    public void modeParsing() {
        assertEquals(Mode.AUTO, ConflictResolvers.parseMode(null));
        assertEquals(Mode.AUTO, ConflictResolvers.parseMode("  "));
        assertEquals(Mode.AUTO, ConflictResolvers.parseMode("auto"));
        assertEquals(Mode.GUI, ConflictResolvers.parseMode("gui"));
        assertEquals(Mode.GUI, ConflictResolvers.parseMode("GUI"));
        assertEquals(Mode.CONSOLE, ConflictResolvers.parseMode("console"));
        assertEquals(Mode.CONSOLE, ConflictResolvers.parseMode(" Console "));
    }

    /** A typo must be reported, not silently treated as the default. */
    @Test
    public void anUnknownModeIsRejected() {
        try {
            ConflictResolvers.parseMode("popup");
            fail("expected the typo to be rejected");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("auto, gui, console"));
        }
    }

    /** Explicitly asking for a dialog on a headless box must degrade, not throw. */
    @Test
    public void guiModeFallsBackWhenHeadless() {
        String headless = System.getProperty("java.awt.headless");
        System.setProperty("java.awt.headless", "true");
        try {
            assertTrue(ConflictResolvers.create(Mode.GUI) instanceof ConsoleConflictResolver);
        } finally {
            if (headless == null) {
                System.clearProperty("java.awt.headless");
            } else {
                System.setProperty("java.awt.headless", headless);
            }
        }
    }
}
