package io.milton.sync;

import java.io.Console;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Console replacement for the original SwingConflictResolver, which popped a modal
 * dialog and so could not work in a headless CLI. Same choices and same
 * "remember my answer for N seconds" behaviour, asked on stdin instead.
 */
public class ConsoleConflictResolver {

    private static final Logger log = LoggerFactory.getLogger(ConsoleConflictResolver.class);

    public enum ConflictChoice {

        LOCAL,
        REMOTE,
        NOTHING
    }

    public ConflictChoice choice = null;
    public Integer rememberSecs;

    private final Console console = System.console();
    // one scanner for the life of the resolver: Scanner buffers ahead, so a new one
    // per prompt swallows the input meant for the next prompt
    private Scanner scanner;

    public ConsoleConflictResolver() {
    }

    /**
     * Ask the user how to resolve a conflict. Synchronized because sync runs on
     * background threads and two prompts interleaved on one terminal are unreadable.
     *
     * @param message describes the conflict
     * @param defaultSecs seconds to remember the answer for, offered as the default
     * @return the choice, or NOTHING if there is no one to ask
     */
    public synchronized ConflictChoice showConflictResolver(String message, Integer defaultSecs) {
        System.out.println();
        System.out.println("CONFLICT: " + message);
        System.out.println("  [l] local   keep the local file, discard the remote change");
        System.out.println("  [r] remote  overwrite the local file with the remote version");
        System.out.println("  [n] nothing leave it for now, ask again next time");

        choice = null;
        while (choice == null) {
            String s = prompt("Choose l, r or n: ");
            if (s == null) {
                // no console and nothing on stdin, so nobody to ask. Changing files
                // unattended risks losing work, so leave it alone.
                log.warn("No console available to resolve conflict, leaving both files unchanged: {}", message);
                return ConflictChoice.NOTHING;
            }
            s = s.trim().toLowerCase();
            if (s.startsWith("l")) {
                choice = ConflictChoice.LOCAL;
            } else if (s.startsWith("r")) {
                choice = ConflictChoice.REMOTE;
            } else if (s.startsWith("n")) {
                choice = ConflictChoice.NOTHING;
            }
        }

        String secs = prompt("Remember this choice for how many seconds? [" + (defaultSecs == null ? "0" : defaultSecs) + "]: ");
        rememberSecs = parseSecs(secs, defaultSecs);

        return choice;
    }

    public Integer getRememberSecs() {
        return rememberSecs;
    }

    private static Integer parseSecs(String s, Integer defaultSecs) {
        if (s == null || s.trim().isEmpty()) {
            return defaultSecs;
        }
        try {
            int i = Integer.parseInt(s.trim());
            return i >= 0 ? i : defaultSecs;
        } catch (NumberFormatException ex) {
            return defaultSecs;
        }
    }

    /**
     * @return the line entered, or null at end of input
     */
    private String prompt(String text) {
        if (console != null) {
            return console.readLine(text);
        }
        System.out.print(text);
        System.out.flush();
        if (scanner == null) {
            scanner = new Scanner(System.in);
        }
        return scanner.hasNextLine() ? scanner.nextLine() : null;
    }
}
