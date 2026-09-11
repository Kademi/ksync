package co.kademi.sync;

import co.kademi.sync.commands.CheckIgnoreCommand;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** What check-ignore reports: which rule won, and which file and line it came from. */
public class CheckIgnoreTest {

    private File root;
    private Path userFile;

    @Before
    public void setUp() throws Exception {
        Path tmp = Files.createTempDirectory("checkignore");
        root = Files.createDirectory(tmp.resolve("checkout")).toFile();
        userFile = tmp.resolve("ignore");
        System.setProperty(GlobalIgnores.PATH_PROPERTY, userFile.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(GlobalIgnores.PATH_PROPERTY);
    }

    private void writeUser(String content) throws Exception {
        Files.write(userFile, content.getBytes(StandardCharsets.UTF_8));
    }

    private void writeCheckout(String content) throws Exception {
        Files.write(new File(root, Ignores.IGNORE_FILE).toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void reportsTheBuiltInThatWon() {
        Ignores.Match m = Ignores.load(root, null).explain("node_modules", true);
        assertEquals(Ignores.BUILT_IN_SOURCE, m.source);
        assertEquals("node_modules", m.pattern);
        assertTrue(m.ignored);
        assertEquals("built-in:node_modules", m.toString());
    }

    /** The line number has to count the blanks and comments, or it points at the wrong rule. */
    @Test
    public void reportsTheFileAndLine_countingBlanksAndComments() throws Exception {
        writeUser("# my machine\n\n*.log\n");
        Ignores.Match m = Ignores.load(root, null).explain("deep/debug.log", false);
        assertEquals(userFile.toString(), m.source);
        assertEquals(3, m.line);
        assertEquals("*.log", m.pattern);
    }

    @Test
    public void reportsTheCheckoutFile_whenItOverridesTheUserFile() throws Exception {
        writeUser("*.log\n");
        writeCheckout("!keep.log\n");
        Ignores.Match m = Ignores.load(root, null).explain("keep.log", false);
        assertEquals(new File(root, Ignores.IGNORE_FILE).getPath(), m.source);
        assertEquals("!keep.log", m.pattern);
        assertEquals("a re-include is still the rule that decided it", false, m.ignored);
    }

    @Test
    public void reportsTheRunPatterns() {
        Ignores.Match m = Ignores.load(root, Arrays.asList("target")).explain("target", true);
        assertEquals(Ignores.RUN_SOURCE, m.source);
        assertEquals("-ignore:target", m.toString());
    }

    @Test
    public void theStateDirectoryIsReportedAsBuiltIn() {
        Ignores.Match m = Ignores.load(root, null).explain(".ksync/ksync.properties", false);
        assertEquals("built-in:.ksync", m.toString());
        assertTrue(m.ignored);
    }

    @Test
    public void nothingMatched_isNull() {
        assertNull(Ignores.load(root, null).explain("theme/app.js", false));
    }

    @Test
    public void pathsAreAcceptedRelativeOrAbsolute() {
        assertEquals("theme/app.js", CheckIgnoreCommand.relativize(root, "theme/app.js"));
        assertEquals("theme/app.js",
                CheckIgnoreCommand.relativize(root, new File(root, "theme/app.js").getAbsolutePath()));
        assertNull("outside the checkout", CheckIgnoreCommand.relativize(root, "/etc/passwd"));
    }
}
