package co.kademi.sync;

import java.util.function.Supplier;
import org.hashsplit4j.api.HashCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HashCache which does not build its delegate until something actually asks it a question.
 * <p>
 * The bloom filter caches answer "does the server already have this hash", which is only useful when deciding what to
 * upload. Building them used to happen in the KSync3 constructor, so every command paid for them - including checkout,
 * which is a pure download and never consults them. Each filter costs the server a full walk of the repository, so on a
 * large repo that was minutes of work thrown away, and often a timeout.
 * <p>
 * Wrapping the filters in this defers that cost to the first push. If the delegate cannot be built we degrade to
 * answering "no", which is the safe direction: the caller uploads something the server may already have, rather than
 * skipping something it does not.
 *
 * @author brad
 */
public class LazyHashCache implements HashCache {

    private static final Logger log = LoggerFactory.getLogger(LazyHashCache.class);

    private final String name;
    private final Supplier<HashCache> supplier;

    private HashCache delegate;
    private boolean initialised;

    public LazyHashCache(String name, Supplier<HashCache> supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public boolean hasHash(String hash) {
        HashCache c = cache();
        return c != null && c.hasHash(hash);
    }

    @Override
    public void setHash(String hash) {
        HashCache c = cache();
        if (c != null) {
            c.setHash(hash);
        }
    }

    private synchronized HashCache cache() {
        if (!initialised) {
            initialised = true;
            try {
                log.info("Fetching {}...", name);
                delegate = supplier.get();
            } catch (Exception e) {
                log.warn("Unable to load {}, so things will be a bit slow: {}", name, e.getMessage());
                delegate = null;
            }
        }
        return delegate;
    }
}
