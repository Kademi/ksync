package co.kademi.sync;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The user level ignore file, and how the three layers stack.
 *
 * Git's order, weakest first: the built in defaults, then this file, then the checkout's own
 * .ksyncignore which the team shares, then whatever -ignore passed for one run. Later wins
 * throughout, which is what lets each layer undo the one above it.
 */
public class GlobalIgnoresTest {

    private Path path;
    private GlobalIgnores ignores;
    private File root;

    @Before
    public void setUp() throws Exception {
        Path tmp = Files.createTempDirectory("ksyncignore");
        path = tmp.resolve("config").resolve("ksync").resolve("ignore");
        ignores = new GlobalIgnores(path);
        root = Files.createDirectory(tmp.resolve("checkout")).toFile();
        System.setProperty(GlobalIgnores.PATH_PROPERTY, path.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(GlobalIgnores.PATH_PROPERTY);
    }

    private void write(String content) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    private void writeCheckoutFile(String content) throws Exception {
        Files.write(new File(root, Ignores.IGNORE_FILE).toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void noFileYet_hasNoPatterns() {
        assertFalse(ignores.exists());
        assertTrue(ignores.patterns().isEmpty());
    }

    @Test
    public void readsPatterns_skippingBlanksAndComments() throws Exception {
        write("# junk we never want\n\n*.log\n  node_modules  \n\n#target\n");
        assertEquals(Arrays.asList("*.log", "  node_modules"), ignores.patterns());
    }

    /** The slash is meaning, not noise, so it reaches the engine intact. */
    @Test
    public void aTrailingSlash_survivesReading() throws Exception {
        write("build/\n");
        assertEquals(Arrays.asList("build/"), ignores.patterns());
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
    }

    @Test
    public void addKeepsCommentsAndExistingPatterns() throws Exception {
        write("# mine\n*.log\n");
        assertTrue(ignores.add("target"));
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        assertTrue(content.contains("# mine"));
        assertEquals(Arrays.asList("*.log", "target"), ignores.patterns());
    }

    /** A file with no final newline must not have the new pattern glued onto the last one. */
    @Test
    public void addToFileWithNoTrailingNewline() throws Exception {
        write("*.log");
        assertTrue(ignores.add("target"));
        assertEquals(Arrays.asList("*.log", "target"), ignores.patterns());
    }

    /** Where git keeps its equivalent, and beside the credentials file. Never ~/.ksyncignore. */
    @Test
    public void theDefaultPath_isUnderTheUserConfigDir() {
        System.clearProperty(GlobalIgnores.PATH_PROPERTY);
        Path p = GlobalIgnores.defaultPath();
        assertEquals("ignore", p.getFileName().toString());
        assertEquals("ksync", p.getParent().getFileName().toString());
    }

    @Test
    public void withNothingConfiguredAnywhere_theBuiltInsStillApply() {
        Ignores all = Ignores.load(root, null);
        assertTrue(all.ignored("node_modules", true));
        assertTrue(all.ignored(".git", true));
        assertTrue(all.ignored("a/b/Thumbs.db", false));
        assertFalse(all.ignored("src/app.js", false));
    }

    @Test
    public void theUserFileAdds_andTheCheckoutFileCanUndoIt() throws Exception {
        write("*.log\n");
        writeCheckoutFile("!keep.log\n");
        Ignores all = Ignores.load(root, null);
        assertTrue(all.ignored("debug.log", false));
        assertFalse("the checkout file is stronger than the user's", all.ignored("keep.log", false));
    }

    /** What this change was asked for: a checkout that really does keep its own node_modules. */
    @Test
    public void theCheckoutFile_canReIncludeABuiltIn() throws Exception {
        writeCheckoutFile("!node_modules\n");
        Ignores all = Ignores.load(root, null);
        assertFalse(all.ignored("node_modules", true));
        assertFalse(all.ignored("node_modules/lib/x.js", false));
        assertTrue("the others are untouched", all.ignored("bower_components", true));
    }

    /** -ignore is applied last, so it beats every file. */
    @Test
    public void theRunOverridesEverything() throws Exception {
        writeCheckoutFile("!node_modules\n");
        Ignores all = Ignores.load(root, Arrays.asList("node_modules"));
        assertTrue(all.ignored("node_modules", true));
    }

    @Test
    public void theStateDirectoryIsNeverSyncable_whateverAnyLayerSays() throws Exception {
        write("!.ksync\n");
        writeCheckoutFile("!.ksync/**\n");
        Ignores all = Ignores.load(root, Arrays.asList("!.ksync"));
        assertTrue(all.ignored(".ksync", true));
        assertTrue(all.ignored(".ksync/ksync.properties", false));
    }

    /** A checkout with no ignore file of its own is the common case and must not fail. */
    @Test
    public void aMissingCheckoutFile_isNotAnError() {
        assertFalse(new File(root, Ignores.IGNORE_FILE).exists());
        assertTrue(Ignores.load(root, null).ignored("node_modules", true));
    }
}
