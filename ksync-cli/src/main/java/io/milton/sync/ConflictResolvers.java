package io.milton.sync;

import java.awt.GraphicsEnvironment;
import java.util.Locale;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Picks the conflict resolver to use.
 *
 * A dialog is the default, because a conflict needs a decision and a prompt buried in scrolling
 * sync output is easy to miss. It is the wrong choice whenever nobody can click it, and that is
 * not the same question as whether a display exists: an agent driving ksync on a developer's
 * desktop has a perfectly good display and still cannot press a button. So the automatic choice
 * looks at who is running, not just at whether X is up.
 */
public class ConflictResolvers {

    private static final Logger log = LoggerFactory.getLogger(ConflictResolvers.class);

    public enum Mode {
        AUTO, GUI, CONSOLE
    }

    private ConflictResolvers() {
    }

    public static Mode parseMode(String s) {
        if (StringUtils.isBlank(s)) {
            return Mode.AUTO;
        }
        try {
            return Mode.valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unknown conflict mode '" + s
                    + "'. Use one of: auto, gui, console");
        }
    }

    public static ConflictResolver create(Mode mode) {
        switch (mode) {
            case CONSOLE:
                return new ConsoleConflictResolver();
            case GUI:
                if (!canShowDialog()) {
                    log.warn("A dialog was requested but this jvm is headless, falling back to the terminal prompt");
                    return new ConsoleConflictResolver();
                }
                return new GuiConflictResolver();
            default:
                String why = whyNotGui(System::getenv, canShowDialog());
                if (why == null) {
                    return new GuiConflictResolver();
                }
                log.debug("Using the terminal prompt for conflicts: {}", why);
                return new ConsoleConflictResolver();
        }
    }

    /**
     * @return the reason a dialog would be wrong here, or null if a dialog is fine. Takes the
     * environment and the display as arguments so it can be tested without either.
     */
    static String whyNotGui(Function<String, String> env, boolean displayAvailable) {
        // An agent runs ksync on a normal desktop, display and all, but cannot click anything.
        // This has to be checked before the display, or the agent gets a dialog it cannot answer
        // and the sync hangs until the user notices a window they did not ask for.
        if (StringUtils.isNotBlank(env.apply("CLAUDECODE"))) {
            return "running under Claude Code";
        }
        if (isTrue(env.apply("KSYNC_NON_INTERACTIVE"))) {
            return "KSYNC_NON_INTERACTIVE is set";
        }
        // CI is set by github actions, gitlab, circleci, travis and jenkins among others
        if (isTrue(env.apply("CI"))) {
            return "CI is set";
        }
        if ("noninteractive".equalsIgnoreCase(env.apply("DEBIAN_FRONTEND"))) {
            return "DEBIAN_FRONTEND is noninteractive";
        }
        if ("dumb".equalsIgnoreCase(env.apply("TERM"))) {
            return "TERM is dumb";
        }
        if (!displayAvailable) {
            return "no display is available";
        }
        return null;
    }

    /**
     * Whether a dialog can actually be put on a screen.
     *
     * The check has to probe rather than infer. On linux the absence of DISPLAY is a reliable
     * signal, but on macOS there is no such variable: a jvm reached over ssh looks identical to
     * one on the desktop until it tries to talk to the window server, and
     * GraphicsEnvironment.isHeadless() does not reliably say so beforehand. Asking for the screen
     * devices forces that connection, so a mac with no window session answers honestly instead of
     * throwing later, in the middle of a sync, from inside a dialog nobody can see.
     */
    private static boolean canShowDialog() {
        // read directly rather than trusting isHeadless(), which caches the property on first use
        if (Boolean.getBoolean("java.awt.headless")) {
            return false;
        }
        try {
            if (GraphicsEnvironment.isHeadless()) {
                return false;
            }
            return GraphicsEnvironment.getLocalGraphicsEnvironment().getScreenDevices().length > 0;
        } catch (Throwable e) {
            // HeadlessException, or on a mac with no window session an AWTError from the
            // native layer. Either way there is nowhere to draw.
            log.debug("No usable display", e);
            return false;
        }
    }

    /** Treats an env var as set unless it says false or 0, matching how CI vars are used. */
    private static boolean isTrue(String v) {
        if (StringUtils.isBlank(v)) {
            return false;
        }
        return !"false".equalsIgnoreCase(v.trim()) && !"0".equals(v.trim());
    }
}
