package co.kademi.sync.oauth;

/**
 * There are no usable credentials for a site and only the user can fix it, by logging in again.
 *
 * Distinct from a transient failure on purpose: a network blip must not tell someone their
 * login is gone, and a dead refresh token must not be reported as something to retry. The CLI
 * prints the message on its own, without a stack trace, because there is nothing in the trace
 * for the person reading it.
 */
public class NotLoggedInException extends RuntimeException {

    public NotLoggedInException(String message) {
        super(message);
    }

    public NotLoggedInException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @return the NotLoggedInException in this chain, or null */
    public static NotLoggedInException find(Throwable t) {
        for (Throwable e = t; e != null; e = e.getCause()) {
            if (e instanceof NotLoggedInException) {
                return (NotLoggedInException) e;
            }
        }
        return null;
    }
}
