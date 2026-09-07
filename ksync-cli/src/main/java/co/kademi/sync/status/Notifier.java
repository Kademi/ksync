package co.kademi.sync.status;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transient desktop notifications, for the moments a persistent icon would not catch: a push that
 * failed, or a remote that -localwins has just overwritten.
 *
 * Uses whatever the platform already has rather than a notification library. On macOS this is the
 * only route worth taking - AWT's TrayIcon.displayMessage has never been dependable there - while
 * on Windows and Linux the tray's own balloon is better when there is a tray, and this is the
 * fallback for when there is not.
 */
public class Notifier {

    private static final Logger log = LoggerFactory.getLogger(Notifier.class);

    private Notifier() {
    }

    public static boolean isMac() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac");
    }

    private static boolean isLinux() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return !isMac() && !os.contains("win");
    }

    /**
     * @return true if a notification was handed to the OS. False means there was nothing to hand
     * it to, and the caller should fall back on the log.
     */
    public static boolean send(String title, String body, boolean problem) {
        try {
            if (isMac()) {
                // osascript is present on every macOS install
                return run("osascript", "-e", "display notification " + asAppleScriptString(body)
                        + " with title " + asAppleScriptString(title));
            }
            if (isLinux()) {
                // Normal urgency even for a problem. A critical notification on GNOME never
                // dismisses itself - it sits on top of whatever you are doing until it is
                // clicked - and the status icon is already the thing that persists. The icon
                // carries the state, this only has to catch the eye once.
                return run("notify-send", "--urgency=normal", "--app-name=ksync",
                        "--icon=" + (problem ? "dialog-error" : "dialog-information"), title, body);
            }
        } catch (RuntimeException ex) {
            log.debug("Could not send a desktop notification", ex);
        }
        return false;
    }

    /**
     * Fire and forget, with a short leash. A notification daemon that hangs must not hang the
     * sync, and its exit code tells us nothing worth waiting on beyond that.
     */
    private static boolean run(String... command) {
        try {
            Process p = new ProcessBuilder(command)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            if (!p.waitFor(3, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                log.debug("Notification command {} did not finish, gave up on it", command[0]);
                return false;
            }
            return p.exitValue() == 0;
        } catch (IOException ex) {
            // the tool is simply not installed, which is normal on a minimal box
            log.debug("Notification command {} is not available", command[0], ex);
            return false;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * AppleScript strings take double quotes with backslash escapes. A file path with a quote in
     * it would otherwise end the string early and run whatever followed as script.
     */
    static String asAppleScriptString(String s) {
        String escaped = s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
        return "\"" + escaped + "\"";
    }
}
