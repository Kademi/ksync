package co.kademi.sync;

import io.milton.sync.Utils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class GlobalIgnoresTest {

    private Path path;
    private GlobalIgnores ignores;

    @Before
    public void setUp() throws Exception {
        path = Files.createTempDirectory("ksyncignore").resolve("home").resolve(".ksyncignore");
        ignores = new GlobalIgnores(path);
    }

    private void write(String content) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void noFileYet_hasNoPatterns() {
        assertFalse(ignores.exists());
        assertTrue(ignores.patterns().isEmpty());
    }

    @Test
    public void readsPatterns_skippingBlanksAndComments() throws Exception {
        write("# junk we never want\n\n*.log\n  node_modules  \n\n#target\n");
        assertEquals(Arrays.asList("*.log", "node_modules"), ignores.patterns());
    }

    @Test
    public void trailingSlashMeansDirectory_andIsDropped() throws Exception {
        write("node_modules/\n");
        assertEquals(Arrays.asList("node_modules"), ignores.patterns());
    }

    @Test
    public void addCreatesTheFile() throws Exception {
        assertTrue(ignores.add("*.log"));
        assertTrue(ignores.exists());
        assertEquals(Arrays.asList("*.log"), ignores.patterns());
    }

    @Test
    public void addIsIdempotent() throws Exception {
        assertTrue(ignores.add("*.log"));
        assertFalse(ignores.add("*.log"));
        assertFalse(ignores.add(" *.log "));
        assertEquals(Arrays.asList("*.log"), ignores.patterns());
    }

    @Test
    public void addKeepsCommentsAndExistingPatterns() throws Exception {
        write("# mine\n*.log\n");
        assertTrue(ignores.add("node_modules"));
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        assertTrue(content.contains("# mine"));
        assertEquals(Arrays.asList("*.log", "node_modules"), ignores.patterns());
    }

    /** A file with no final newline must not have the new pattern glued onto the last one */
    @Test
    public void addToFileWithNoTrailingNewline() throws Exception {
        write("*.log");
        assertTrue(ignores.add("node_modules"));
        assertEquals(Arrays.asList("*.log", "node_modules"), ignores.patterns());
    }

    @Test
    public void combineMergesGlobalAndLocal_withoutDuplicates() throws Exception {
        write("*.log\nnode_modules\n");
        List<String> combined = GlobalIgnores.combine(ignores, Arrays.asList("target", "*.log"));
        assertEquals(Arrays.asList("*.log", "node_modules", "target"), combined);
    }

    /** Downstream ignore checks read null as "nothing to ignore", so preserve that */
    @Test
    public void combineWithNothingAnywhereIsNull() {
        assertNull(GlobalIgnores.combine(ignores, null));
    }

    @Test
    public void combineWithNoGlobalFileKeepsLocal() {
        assertEquals(Arrays.asList("target"), GlobalIgnores.combine(ignores, Arrays.asList("target")));
    }

    @Test
    public void unreadableFileDoesNotStopTheSync() throws Exception {
        Files.createDirectories(path); // a directory where a file should be, so reading it fails
        assertTrue(ignores.patterns().isEmpty());
    }

    @Test
    public void defaultPathHonoursTheOverride() {
        String old = System.getProperty(GlobalIgnores.PATH_PROPERTY);
        try {
            System.setProperty(GlobalIgnores.PATH_PROPERTY, "/tmp/some-ignore-file");
            assertEquals("/tmp/some-ignore-file", GlobalIgnores.defaultPath().toString());
        } finally {
            if (old == null) {
                System.clearProperty(GlobalIgnores.PATH_PROPERTY);
            } else {
                System.setProperty(GlobalIgnores.PATH_PROPERTY, old);
            }
        }
    }

    @Test
    public void defaultPathIsInTheHomeDirectory() {
        String old = System.getProperty(GlobalIgnores.PATH_PROPERTY);
        System.clearProperty(GlobalIgnores.PATH_PROPERTY);
        try {
            Path p = GlobalIgnores.defaultPath();
            assertEquals(".ksyncignore", p.getFileName().toString());
            assertEquals(System.getProperty("user.home"), p.getParent().toString());
        } finally {
            if (old != null) {
                System.setProperty(GlobalIgnores.PATH_PROPERTY, old);
            }
        }
    }

    // ---- the matching the patterns get downstream ----

    @Test
    public void globPatternsMatch() {
        List<String> patterns = Arrays.asList("*.log", "node_modules", "build-?");
        assertTrue(Utils.matchesAny("debug.log", patterns));
        assertTrue(Utils.matchesAny("node_modules", patterns));
        assertTrue(Utils.matchesAny("build-1", patterns));
        assertFalse(Utils.matchesAny("debug.txt", patterns));
        assertFalse(Utils.matchesAny("build-12", patterns));
    }

    /** A leading * is not a legal regex, and used to blow up String.matches */
    @Test
    public void globDoesNotThrow() {
        assertTrue(Utils.matches("a.log", "*.log"));
    }

    /** Patterns people already have in -ignore are regexes, so they must keep working */
    @Test
    public void regexPatternsStillMatch() {
        assertTrue(Utils.matches("thing.tmp", ".*\\.tmp"));
    }

    @Test
    public void nonsensePatternIsSkippedRatherThanThrowing() {
        assertFalse(Utils.matches("anything", "[unclosed"));
    }

    @Test
    public void nullsAreSafe() {
        assertFalse(Utils.matchesAny("x", null));
        assertFalse(Utils.matchesAny(null, Arrays.asList("*.log")));
        assertFalse(Utils.matches("x", null));
    }

    /** The ignore list reaches the file scan through Utils.ignored */
    @Test
    public void ignoredFileUsesGlobPatterns() throws Exception {
        File dir = Files.createTempDirectory("scan").toFile();
        File log = new File(dir, "debug.log");
        File keep = new File(dir, "index.html");
        assertTrue(log.createNewFile());
        assertTrue(keep.createNewFile());
        List<String> patterns = Arrays.asList("*.log");
        assertTrue(Utils.ignored(log, patterns));
        assertFalse(Utils.ignored(keep, patterns));
    }
}
