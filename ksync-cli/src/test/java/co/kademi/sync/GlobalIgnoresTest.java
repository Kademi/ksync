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
        assertTrue(ignores.add("target"));
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        assertTrue(content.contains("# mine"));
        assertEquals(Arrays.asList("*.log", "target"), ignores.patterns());
    }

    /** A file with no final newline must not have the new pattern glued onto the last one */
    @Test
    public void addToFileWithNoTrailingNewline() throws Exception {
        write("*.log");
        assertTrue(ignores.add("target"));
        assertEquals(Arrays.asList("*.log", "target"), ignores.patterns());
    }

    @Test
    public void combineMergesGlobalAndLocal_withoutDuplicates() throws Exception {
        write("*.log\nnode_modules\n");
        List<String> combined = GlobalIgnores.combine(ignores, Arrays.asList("target", "*.log"));
        // node_modules is built in, so naming it in the file changes nothing about the order
        assertEquals(withBuiltIns("*.log", "target"), combined);
    }

    /**
     * The whole point of the built ins: a developer who has configured nothing, on a machine with
     * no ignore file, still does not sync node_modules.
     */
    @Test
    public void theBuiltInsApplyWithNothingConfiguredAnywhere() {
        List<String> combined = GlobalIgnores.combine(ignores, null);
        assertEquals(GlobalIgnores.BUILT_IN, combined);
        assertTrue(Utils.matchesAny("node_modules", combined));
    }

    @Test
    public void combineWithNoGlobalFileKeepsLocal() {
        assertEquals(withBuiltIns("target"), GlobalIgnores.combine(ignores, Arrays.asList("target")));
    }

    /** Built ins come first, so a list read back is ordered by how hard it is to change. */
    @Test
    public void theBuiltInsComeFirst() throws Exception {
        write("aaa-sorts-before-everything\n");
        List<String> combined = GlobalIgnores.combine(ignores, Arrays.asList("zzz"));
        assertEquals(GlobalIgnores.BUILT_IN, combined.subList(0, GlobalIgnores.BUILT_IN.size()));
    }

    /**
     * Adding one to the file would write a line that does nothing, and then read back as though
     * the file were what was protecting you.
     */
    @Test
    public void addingABuiltInIsANoOp() throws Exception {
        assertFalse(ignores.add("node_modules"));
        assertFalse("a trailing slash is the same pattern", ignores.add("node_modules/"));
        assertFalse("nothing should have been written", ignores.exists());
        assertTrue(GlobalIgnores.isBuiltIn("node_modules"));
        assertTrue(GlobalIgnores.isBuiltIn(" node_modules/ "));
        assertFalse(GlobalIgnores.isBuiltIn("target"));
        assertFalse(GlobalIgnores.isBuiltIn(null));
    }

    /**
     * Kept deliberately short, because a built in cannot be turned off. dist, build and target
     * are build output in most projects but a real part of an app in some, and silently refusing
     * to deploy one would be much harder to work out than a large first sync.
     */
    @Test
    public void theBuiltInsAreLimitedToWhatIsNeverDeployable() {
        assertTrue(GlobalIgnores.BUILT_IN.contains("node_modules"));
        for (String risky : Arrays.asList("dist", "build", "target", "out", "vendor", "assets")) {
            assertFalse(risky + " must not be built in, it can be part of an app",
                    GlobalIgnores.BUILT_IN.contains(risky));
        }
    }

    /** The built ins reach the scan, which is the check that actually skips a directory. */
    @Test
    public void theScanSkipsTheBuiltIns() {
        List<String> combined = GlobalIgnores.combine(ignores, null);
        assertTrue(Utils.ignored(new java.io.File("/some/app/node_modules"), combined));
        assertTrue(Utils.ignored(new java.io.File("/some/app/bower_components"), combined));
        assertTrue(Utils.ignored(new java.io.File("/some/app/Thumbs.db"), combined));
        assertFalse(Utils.ignored(new java.io.File("/some/app/theme.css"), combined));
    }

    private static List<String> withBuiltIns(String... rest) {
        List<String> all = new java.util.ArrayList<>(GlobalIgnores.BUILT_IN);
        all.addAll(Arrays.asList(rest));
        return all;
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
