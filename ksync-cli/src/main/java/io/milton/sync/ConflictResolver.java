package io.milton.sync;

/**
 * Asks a person how to resolve a file conflict. There are two, chosen by
 * {@link ConflictResolvers}: a desktop dialog and a terminal prompt.
 */
public interface ConflictResolver {

    enum ConflictChoice {
        LOCAL,
        REMOTE,
        NOTHING
    }

    /**
     * @param message describes the conflict
     * @param defaultSecs seconds to remember the answer for, offered as the default
     * @return the choice, or NOTHING when there is nobody to ask
     */
    ConflictChoice showConflictResolver(String message, Integer defaultSecs);

    /**
     * @return how long the caller should reuse the last answer for, or null to ask every time
     */
    Integer getRememberSecs();
}
