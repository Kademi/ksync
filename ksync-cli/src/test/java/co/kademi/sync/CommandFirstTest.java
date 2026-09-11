package co.kademi.sync;

import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

/**
 * ksync:// links and older scripts pass the command as -command x, and both are out in the world
 * where they cannot be edited.
 */
public class CommandFirstTest {

    @Test
    public void theCommandOptionBecomesABareCommand() {
        assertArrayEquals(new String[]{"sync", "-appdir", "/tmp/x"},
                Cli.commandFirst(new String[]{"-command", "sync", "-appdir", "/tmp/x"}));
    }

    @Test
    public void theCommandOptionIsFoundAfterOtherOptions() {
        assertArrayEquals(new String[]{"pull", "-debug"},
                Cli.commandFirst(new String[]{"-debug", "-command", "pull"}));
    }

    /** The single dash long form is what the docs and the ksync:// links use. */
    @Test
    public void singleDashLongNames_areTranslated() {
        java.util.Set<String> known = new java.util.HashSet<>(java.util.Arrays.asList("--url", "--localwins"));
        assertArrayEquals(new String[]{"pull", "--url", "http://x"},
                Cli.longFormAliases(new String[]{"pull", "-url", "http://x"}, known));
        assertArrayEquals(new String[]{"pull", "--url=http://x"},
                Cli.longFormAliases(new String[]{"pull", "-url=http://x"}, known));
        assertArrayEquals(new String[]{"pull", "--localwins"},
                Cli.longFormAliases(new String[]{"pull", "-localwins"}, known));
    }

    /** A cluster of short options is not a long name, and must survive untouched. */
    @Test
    public void shortOptionsAndClusters_areLeftAlone() {
        java.util.Set<String> known = new java.util.HashSet<>(java.util.Arrays.asList("--url", "--debug", "--localwins"));
        assertArrayEquals(new String[]{"pull", "-dl"}, Cli.longFormAliases(new String[]{"pull", "-dl"}, known));
        assertArrayEquals(new String[]{"pull", "-d"}, Cli.longFormAliases(new String[]{"pull", "-d"}, known));
        assertArrayEquals(new String[]{"pull", "--url"}, Cli.longFormAliases(new String[]{"pull", "--url"}, known));
        assertArrayEquals(new String[]{"pull", "-unknown"},
                Cli.longFormAliases(new String[]{"pull", "-unknown"}, known));
    }

    @Test
    public void aBareCommandIsLeftAlone() {
        assertArrayEquals(new String[]{"pull", "--debug"}, Cli.commandFirst(new String[]{"pull", "--debug"}));
        assertArrayEquals(new String[]{}, Cli.commandFirst(new String[]{}));
    }
}
