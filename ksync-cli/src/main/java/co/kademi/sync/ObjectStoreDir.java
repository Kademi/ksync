package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where the blobs and fanouts a checkout has fetched are kept.
 *
 * They used to live in the checkout itself, under .ksync/blobs and .ksync/hashes. Both are content
 * addressed and fan the hash out over nested directories, so a checkout of any size leaves tens of
 * thousands of tiny files inside the folder being worked in, which editors index and other sync
 * tools copy. ksync's own scan never looked at them, but everything else on the machine did.
 *
 * Keyed on the repository, like the file hash cache, so two checkouts of different versions of one
 * repository share their objects rather than each fetching the same blob. Content addressing is
 * what makes that safe: the same hash is the same bytes whoever wrote it.
 */
public class ObjectStoreDir {

    private static final Logger log = LoggerFactory.getLogger(ObjectStoreDir.class);

    /** Overrides the root the objects are kept under. Set by tests, and usable to relocate them. */
    public static final String PATH_PROPERTY = "ksync.objectsDir";

    /** Names the two stores, in both the old location and the new one. */
    public static final String BLOBS = "blobs";
    public static final String HASHES = "hashes";

    private ObjectStoreDir() {
    }

    /**
     * The directory holding this repository's objects, created if it is not there yet.
     *
     * @param repoKey the repository url, or the version url when not following a repository
     */
    public static File forRepo(String repoKey) {
        File dir = new File(root().toFile(), KSync3Utils.makeFileName(repoKey));
        dir.mkdirs();
        return dir;
    }

    /** ~/.cache/ksync/objects, or the platform equivalent. */
    static Path root() {
        String override = System.getProperty(PATH_PROPERTY);
        if (StringUtils.isNotBlank(override)) {
            return Paths.get(override);
        }
        return CredentialStore.userCacheDir().resolve("ksync").resolve("objects");
    }

    /**
     * Moves a checkout's objects out of its .ksync folder, once.
     *
     * Existing checkouts are the ones already carrying the files, so leaving them to be noticed by
     * hand would leave the problem exactly where it is felt. Nothing is re-fetched: the objects are
     * good, only badly placed.
     *
     * A failure here is not fatal. The worst case is that the old directory stays where it is and
     * its objects are fetched again into the new one, which costs a download and not correctness.
     */
    public static void migrate(File configDir, File objectsDir) {
        move(new File(configDir, BLOBS), new File(objectsDir, BLOBS));
        move(new File(configDir, HASHES), new File(objectsDir, HASHES));
    }

    private static void move(File from, File to) {
        if (!from.isDirectory()) {
            return;
        }
        try {
            if (!to.exists()) {
                // The ordinary case, and a rename rather than a walk of every object
                Files.createDirectories(to.toPath().getParent());
                try {
                    Files.move(from.toPath(), to.toPath());
                    log.info("Moved {} to {}", from, to);
                    return;
                } catch (IOException ex) {
                    // Different filesystems, so it has to be copied
                    log.debug("Could not rename {}, copying instead: {}", from, ex.toString());
                }
            }
            // Another checkout of this repository has already made one. Merging is safe because
            // both sides are content addressed: a hash that is in both is the same bytes in both.
            FileUtils.copyDirectory(from, to);
            FileUtils.deleteDirectory(from);
            log.info("Moved {} into {}", from, to);
        } catch (IOException ex) {
            log.warn("Could not move {} to {}, leaving it where it is: {}", from, to, ex.toString());
        }
    }
}
