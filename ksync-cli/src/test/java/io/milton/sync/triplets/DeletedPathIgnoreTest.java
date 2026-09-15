package io.milton.sync.triplets;

import co.kademi.sync.Ignores;
import io.milton.event.EventManagerImpl;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * A delete event arrives after the path is gone, so File.isDirectory is false for what was a
 * directory. A directory-only rule like "build/" would then stop covering it just as the delete
 * needs suppressing, and the sync would push a removal of something it never pushed.
 */
public class DeletedPathIgnoreTest {

    private Path root;

    @Before
    public void setUp() throws Exception {
        root = Files.createTempDirectory("deletedignore");
    }

    private MemoryLocalTripletStore storeWith(Ignores ignores) throws Exception {
        Path work = Files.createTempDirectory("deletedignorework");
        return new MemoryLocalTripletStore(root.toFile(), new EventManagerImpl(),
                new FileSystem2BlobStore(new File(work.toFile(), "blobs")),
                new FileSystem2HashStore(new File(work.toFile(), "hashes")),
                null, null, null, ignores, null);
    }

    @Test
    public void aDirectoryOnlyRule_stillCoversTheDirectoryOnceItIsGone() throws Exception {
        MemoryLocalTripletStore store = storeWith(Ignores.of("build/"));
        File gone = new File(root.toFile(), "build");
        assertFalse("nothing there to call a directory", gone.exists());
        assertTrue(store.ignored(gone));
    }

    @Test
    public void aDirectoryOnlyRule_stillCoversAnExistingDirectory() throws Exception {
        MemoryLocalTripletStore store = storeWith(Ignores.of("build/"));
        File dir = Files.createDirectory(root.resolve("build")).toFile();
        assertTrue(store.ignored(dir));
    }

    @Test
    public void aDirectoryOnlyRule_doesNotCoverAFileOfThatNameWhileItIsThere() throws Exception {
        MemoryLocalTripletStore store = storeWith(Ignores.of("build/"));
        Path file = root.resolve("build");
        Files.write(file, "x".getBytes(StandardCharsets.UTF_8));
        assertFalse(store.ignored(file.toFile()));
    }

    @Test
    public void aPathNoRuleCovers_isStillNotIgnoredOnceItIsGone() throws Exception {
        MemoryLocalTripletStore store = storeWith(Ignores.of("build/"));
        assertFalse(store.ignored(new File(root.toFile(), "theme/app.js")));
    }

    @Test
    public void aReIncludedPath_isStillNotIgnoredOnceItIsGone() throws Exception {
        MemoryLocalTripletStore store = storeWith(Ignores.of("build/", "!build"));
        assertFalse(store.ignored(new File(root.toFile(), "build")));
    }
}
