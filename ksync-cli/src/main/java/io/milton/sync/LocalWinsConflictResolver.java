package io.milton.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Answers every conflict with the local file, without asking.
 *
 * For the common case where the local checkout is version managed and is therefore the authority.
 * The remote differing from local is normal there, so a prompt per file is disruptive when the
 * answer is always the same one. Selected by -localwins; see {@link ConflictResolvers}.
 */
public class LocalWinsConflictResolver implements ConflictResolver {

    private static final Logger log = LoggerFactory.getLogger(LocalWinsConflictResolver.class);

    @Override
    public ConflictChoice showConflictResolver(String message, Integer defaultSecs) {
        // Logged rather than prompted: nobody has a decision to make, but they should still be
        // able to see afterwards which remote changes were discarded.
        log.info("Keeping the local file and discarding the remote change: {}", message);
        return ConflictChoice.LOCAL;
    }

    @Override
    public Integer getRememberSecs() {
        // Nothing to remember, and remembering would stop each conflict being logged
        return null;
    }
}
