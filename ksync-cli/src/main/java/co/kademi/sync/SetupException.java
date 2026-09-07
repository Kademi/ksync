package co.kademi.sync;

/**
 * Something about how this checkout is set up is wrong, and only the person running the command
 * can fix it: no url to sync with, or a url that is not a url.
 *
 * Like NotLoggedInException, the CLI prints the message on its own. A stack trace through the
 * option handling tells the reader nothing they can act on, and buries the one line that does.
 */
public class SetupException extends RuntimeException {

    public SetupException(String message) {
        super(message);
    }

    public SetupException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @return the SetupException in this chain, or null */
    public static SetupException find(Throwable t) {
        for (Throwable e = t; e != null; e = e.getCause()) {
            if (e instanceof SetupException) {
                return (SetupException) e;
            }
        }
        return null;
    }
}
