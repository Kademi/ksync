package co.kademi.sync;

import java.io.Console;
import java.lang.reflect.Method;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How a sync is told to stop from the keyboard.
 *
 * ctrl-c is the way to stop a foreground process, and where it arrives it is handled: the jvm runs
 * its shutdown hooks, the status file is left saying stopped and the icon goes. It does not always
 * arrive. On Windows ksync3 is a batch file which runs java, so the interrupt goes to cmd as well
 * as to the jvm, and in PowerShell it is reported to reach neither - leaving a sync that can only
 * be stopped by closing the window or finding the process.
 *
 * So there is a second way in, over stdin, which is not a signal and cannot be intercepted by
 * whatever is between the terminal and the jvm. Only where someone is actually typing: a sync
 * started from a script or a launcher has no terminal, and reading a stdin nobody is at would see
 * end of input immediately and stop the sync that was meant to keep running.
 */
public class StopKey {

    private static final Logger log = LoggerFactory.getLogger(StopKey.class);

    /** Said once, after the initial scan, where it is the last thing on screen while watching. */
    public static final String PROMPT = "Press q then Enter to stop, or ctrl-c.";

    /**
     * Whether a typed line is asking for the sync to stop.
     *
     * More than one word for it, because the whole point is to be reachable: someone whose ctrl-c
     * did nothing will try the word that comes to mind, and being told "unknown command" by a
     * program that will not stop is not an improvement.
     */
    static boolean isStop(String line) {
        String s = StringUtils.trimToEmpty(line);
        return s.equalsIgnoreCase("q") || s.equalsIgnoreCase("quit")
                || s.equalsIgnoreCase("exit") || s.equalsIgnoreCase("stop");
    }

    /**
     * The console someone is typing at, or null when there is nobody there.
     *
     * Java 22 changed System.console() to answer with a console even when the streams are
     * redirected, so non-null stopped being proof of a terminal and isTerminal() became the
     * question to ask. It does not exist before 22, where non-null did mean a terminal, and this
     * is compiled for 11 - hence the reflection rather than a call.
     */
    static Console terminal() {
        Console console = System.console();
        if (console == null) {
            return null;
        }
        try {
            Method isTerminal = Console.class.getMethod("isTerminal");
            return (Boolean) isTerminal.invoke(console) ? console : null;
        } catch (NoSuchMethodException ex) {
            return console;
        } catch (ReflectiveOperationException | RuntimeException ex) {
            log.debug("Could not ask the console whether it is a terminal, taking it as one", ex);
            return console;
        }
    }

    /**
     * Holds the calling thread while the watcher works, and returns when the sync should end.
     *
     * @return the exit code for the command
     */
    public static int waitUntilStopped() {
        Console console = terminal();
        if (console == null) {
            log.debug("No terminal, so there is nobody to press a key: waiting to be stopped from outside");
            return sleepUntilInterrupted();
        }
        log.info(PROMPT);
        return readUntilStopped(console::readLine);
    }

    /**
     * @param lines where typed lines come from, one call per line, null at end of input
     */
    static int readUntilStopped(Supplier<String> lines) {
        while (true) {
            String line = lines.get();
            if (line == null) {
                // ctrl-d, ctrl-z, or the terminal going away. Nobody is left to ask, and carrying
                // on would be a sync with no way to stop it short of killing the process.
                log.info("Input ended, so stopping.");
                return 0;
            }
            if (isStop(line)) {
                log.info("Stopping.");
                return 0;
            }
            if (StringUtils.isNotBlank(line)) {
                log.info(PROMPT);
            }
        }
    }

    /** What a sync did before there was a key for it, and still does with no terminal. */
    private static int sleepUntilInterrupted() {
        try {
            while (true) {
                Thread.sleep(200);
            }
        } catch (InterruptedException ex) {
            return 0;
        }
    }

    private StopKey() {
    }
}
