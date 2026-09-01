package io.milton.sync.triplets;

import io.milton.event.EventManagerImpl;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The scan prints a dot per file with no newline, so without closing the line off the next log
 * line lands on the end of it: "/////..........///No change on server since last pull".
 */
public class ScanProgressTest {

    private final PrintStream realOut = System.out;
    private ByteArrayOutputStream captured;
    private Path root;

    @Before
    public void setUp() throws Exception {
        captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, "UTF-8"));
        root = Files.createTempDirectory("scanprogress");
        Files.write(root.resolve("a.txt"), "a".getBytes(StandardCharsets.UTF_8));
        Files.write(root.resolve("b.txt"), "b".getBytes(StandardCharsets.UTF_8));
        Files.createDirectory(root.resolve("sub"));
        Files.write(root.resolve("sub").resolve("c.txt"), "c".getBytes(StandardCharsets.UTF_8));
    }

    @After
    public void tearDown() {
        System.setOut(realOut);
    }

    private MemoryLocalTripletStore storeOver(Path dir) throws Exception {
        Path work = Files.createTempDirectory("scanwork");
        return new MemoryLocalTripletStore(dir.toFile(), new EventManagerImpl(),
                new FileSystem2BlobStore(new File(work.toFile(), "blobs")),
                new FileSystem2HashStore(new File(work.toFile(), "hashes")),
                null, null, null, null, new MapHashCache());
    }

    /** SyncHashCache backed by a map, so the scan does real work without a berkeley db. */
    private static class MapHashCache implements SyncHashCache {

        private final java.util.Map<String, String> map = new java.util.HashMap<>();

        @Override
        public String get(File file) {
            return map.get(file.getAbsolutePath());
        }

        @Override
        public void put(File file, String hash) {
            map.put(file.getAbsolutePath(), hash);
        }
    }

    @Test
    public void scanEndsOnANewLine() throws Exception {
        storeOver(root).scan();

        String out = captured.toString("UTF-8");
        assertTrue("expected progress marks, got: " + out, out.contains("."));
        assertTrue("progress output must end with a newline, got: " + out.replace("\n", "\\n"),
                out.endsWith("\n"));
    }

    /** Whatever prints next must start on its own line, not on the end of the dots. */
    @Test
    public void nextOutputStartsOnItsOwnLine() throws Exception {
        storeOver(root).scan();
        System.out.print("No change on server since last pull");

        String out = captured.toString("UTF-8");
        String lastLine = out.substring(out.lastIndexOf('\n') + 1);
        assertEquals("No change on server since last pull", lastLine);
        assertFalse("the message must not be stuck to the dots", lastLine.contains("."));
    }

    /** Scanning twice must not leave a trailing blank line each time. */
    @Test
    public void doesNotEmitABlankLineWhenThereWasNoProgress() throws Exception {
        MemoryLocalTripletStore s = storeOver(root);
        s.scan();
        captured.reset();
        s.scan();

        String out = captured.toString("UTF-8");
        assertFalse("a scan that printed marks should not start with a bare newline: "
                + out.replace("\n", "\\n"), out.startsWith("\n"));
        assertTrue(out.endsWith("\n"));
        assertEquals("exactly one line of progress", 1, out.split("\n", -1).length - 1);
    }
}
