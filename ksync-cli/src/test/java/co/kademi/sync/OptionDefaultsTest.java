package co.kademi.sync;

import co.kademi.sync.commands.PullCommand;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

/**
 * What a checkout's properties file is allowed to fill in.
 *
 * Only the keys a checkout records. Anything else would be a config file in disguise, and for a
 * negatable option it would invert the flag: picocli toggles those against their default, so a
 * default of true makes --localwins mean false.
 */
public class OptionDefaultsTest {

    private Path dir;

    @Before
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("optdefaults");
        Files.createDirectory(dir.resolve(".ksync"));
        Files.write(dir.resolve(".ksync").resolve("ksync.properties"),
                ("url=https://acme.kademi.co/repositories/site/version1\n"
                + "user=wesley\n"
                + "localwins=true\n"
                + "debug=true\n").getBytes(StandardCharsets.UTF_8));
    }

    private PullCommand parse(String... args) {
        String[] all = new String[args.length + 3];
        all[0] = "pull";
        all[1] = "--appdir";
        all[2] = dir.toString();
        System.arraycopy(args, 0, all, 3, args.length);
        CommandLine cl = new CommandLine(new Cli.Root()).setDefaultValueProvider(new Cli.CheckoutDefaults());
        return (PullCommand) cl.parseArgs(all).subcommand().commandSpec().userObject();
    }

    @Test
    public void theUrlComesFromTheCheckout() {
        assertEquals("https://acme.kademi.co/repositories/site/version1", parse().connection.url);
    }

    /**
     * The username cannot come from here, because --user sits in an exclusive group and picocli
     * never builds a group nothing matched. KSyncUtils.withKsync reads it from the same file
     * instead, which is why that fallback has to stay.
     */
    @Test
    public void theUsernameIsNotFilledInByTheParser() {
        assertNull(parse().connection.user());
    }

    @Test
    public void aFlagInThePropertiesFileIsIgnored() {
        PullCommand cmd = parse();
        assertFalse("localwins must not come from the properties file", cmd.conflictOptions.localwins);
        assertFalse("nor debug", cmd.global.debug);
    }

    /** With a static default the negatable form reads the way it is written, in both directions. */
    @Test
    public void localwinsReadsTheWayItIsWritten() {
        assertTrue(parse("-l").conflictOptions.localwins);
        assertTrue(parse("--localwins").conflictOptions.localwins);
        assertFalse(parse("--no-localwins").conflictOptions.localwins);
        assertFalse(parse().conflictOptions.localwins);
    }

    @Test
    public void theCommandLineStillBeatsTheCheckout() {
        assertEquals("https://other.kademi.co/repositories/x/v1",
                parse("--url", "https://other.kademi.co/repositories/x/v1").connection.url);
    }

    /** appdir is how the ksync:// flow points at another directory, and defaults must follow it. */
    @Test
    public void defaultsComeFromTheDirectoryAppdirNames() {
        assertEquals(dir.toFile(), parse().dir());
        assertTrue(new File(parse().dir(), ".ksync").isDirectory());
    }
}
