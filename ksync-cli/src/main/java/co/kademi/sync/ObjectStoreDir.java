package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.utils.StringFanoutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Moves objects from the older layouts into a checkout's pack store, and hides .ksync from search tools and git. */
public class ObjectStoreDir {

    private static final Logger log = LoggerFactory.getLogger(ObjectStoreDir.class);

    /** Overrides where the old shared store is looked for. Set by tests. */
    public static final String PATH_PROPERTY = "ksync.objectsDir";

    /** The old layout's two stores, in .ksync or the shared store. */
    public static final String BLOBS = "blobs";
    public static final String HASHES = "hashes";

    /** Both: ripgrep reads .gitignore only inside a git repo, and Claude Code's grep reads .gitignore but not .ignore. */
    static final String[] IGNORE_FILES = {".gitignore", ".ignore"};
    static final String IGNORE_ALL = "# Written by ksync3: everything here is its own state, so search tools and git skip it\n*\n";

    private ObjectStoreDir() {
    }

    /** Only where missing, so an edited one is left alone. */
    public static void hideFromTools(File configDir) {
        for (String name : IGNORE_FILES) {
            File f = new File(configDir, name);
            try {
                Files.write(f.toPath(), IGNORE_ALL.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
            } catch (FileAlreadyExistsException ex) {
                // someone else's, or ours from an earlier run
            } catch (IOException ex) {
                log.warn("Could not write {}: {}", f, ex.toString());
            }
        }
    }

    /** Copies everything, not just the synced tree, which is incomplete locally wherever ignores applied. */
    public static void migrate(PackStore packs, File configDir, String repoKey) {
        File blobs = new File(configDir, BLOBS);
        File hashes = new File(configDir, HASHES);
        if (blobs.isDirectory() || hashes.isDirectory()) {
            if (copyInto(packs, blobs, hashes)) {
                try {
                    FileUtils.deleteDirectory(blobs);
                    FileUtils.deleteDirectory(hashes);
                } catch (IOException ex) {
                    log.warn("Could not remove {} and {}, which are no longer used: {}", blobs, hashes, ex.toString());
                }
            }
            return;
        }
        if (!packs.isNew() || KSyncUtils.getLastRemoteHash(configDir) == null) {
            return;
        }
        File shared = new File(sharedRoot().toFile(), KSync3Utils.makeFileName(repoKey));
        // Never deleted: other checkouts of the repository may still read it
        if (shared.isDirectory() && copyInto(packs, new File(shared, BLOBS), new File(shared, HASHES))) {
            log.info("{} is shared by every checkout of this repository, so it has been left there: delete it"
                    + " once they have all run once", shared);
        }
    }

    /** @return whether everything that was copied is in the index on disk */
    private static boolean copyInto(PackStore packs, File blobs, File hashes) {
        log.info("Moving the objects in {} and {} into {}", blobs, hashes, PackStore.DIR);
        List<String> blobHashes = new ArrayList<>();
        List<String> chunkHashes = new ArrayList<>();
        List<String> fileHashes = new ArrayList<>();
        int[] skipped = {0};
        try {
            eachFile(blobs, (name, bytes) -> {
                if (PackStore.keyFor(PackStore.NS_BLOB, name) != null && PackStore.hashesTo(name, bytes)) {
                    packs.setBlob(name, bytes);
                    blobHashes.add(name);
                } else {
                    skipped[0]++;
                }
            });
            // FileSystem2HashStore keeps each kind under its own prefix
            eachFile(new File(hashes, "chunks"), (name, bytes) -> {
                Fanout f = parseCommaFanout(name, bytes);
                if (f == null) {
                    skipped[0]++;
                } else {
                    packs.setChunkFanout(name, f.getHashes(), f.getActualContentLength());
                    chunkHashes.add(name);
                }
            });
            eachFile(new File(hashes, "files"), (name, bytes) -> {
                Fanout f = parseCommaFanout(name, bytes);
                if (f == null) {
                    skipped[0]++;
                } else {
                    packs.setFileFanout(name, f.getHashes(), f.getActualContentLength());
                    fileHashes.add(name);
                }
            });
            packs.flush();
            boolean complete = packs.allOnDisk(PackStore.NS_BLOB, blobHashes)
                    && packs.allOnDisk(PackStore.NS_CHUNK, chunkHashes)
                    && packs.allOnDisk(PackStore.NS_FILE, fileHashes);
            log.info("Moved {} objects{}", blobHashes.size() + chunkHashes.size() + fileHashes.size(),
                    skipped[0] == 0 ? "" : ", leaving " + skipped[0] + " damaged ones to be fetched again");
            if (!complete) {
                log.warn("The pack index does not have everything just copied into it, so {} and {} are kept", blobs, hashes);
            }
            return complete;
        } catch (IOException | RuntimeException ex) {
            log.warn("Could not move the objects in {} and {} into packs, so they are kept: {}", blobs, hashes, ex.toString());
            return false;
        }
    }

    /** Null when it is not a hash this store can hold, or not a fanout in the comma format. */
    private static Fanout parseCommaFanout(String name, byte[] bytes) {
        if (PackStore.keyFor(PackStore.NS_CHUNK, name) == null) {
            return null;
        }
        try {
            Fanout f = StringFanoutUtils.parseFanout(new String(bytes, StandardCharsets.UTF_8));
            return f.getActualContentLength() >= 0 ? f : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private interface ObjectFile {

        void accept(String name, byte[] bytes) throws IOException;
    }

    /** Each file under dir, named by the hash it holds, as FileSystem2Utils lays them out. */
    private static void eachFile(File dir, ObjectFile f) throws IOException {
        if (!dir.isDirectory()) {
            return;
        }
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            for (Path p : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                f.accept(p.getFileName().toString(), Files.readAllBytes(p));
            }
        }
    }

    /** ~/.cache/ksync/objects, or the platform equivalent: where the shared store was kept. */
    static Path sharedRoot() {
        String override = System.getProperty(PATH_PROPERTY);
        if (StringUtils.isNotBlank(override)) {
            return Paths.get(override);
        }
        return CredentialStore.userCacheDir().resolve("ksync").resolve("objects");
    }
}
