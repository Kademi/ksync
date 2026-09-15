package co.kademi.sync;

import co.kademi.sync.commands.PublishCommand;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

/**
 * The ids are compared with equals against a directory name, so space left around one does not
 * fail, it silently publishes nothing for that app. The option's own help writes the list with
 * spaces after the commas, which is how it was being given.
 */
public class AppIdsTest {

    private Path dir;

    @Before
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("appids");
        Files.createDirectory(dir.resolve(".ksync"));
        Files.write(dir.resolve(".ksync").resolve("ksync.properties"),
                "url=https://acme.kademi.co/repositories/site/version1\n".getBytes(StandardCharsets.UTF_8));
    }

    private PublishCommand parse(String... args) {
        String[] all = new String[args.length + 3];
        all[0] = "publish";
        all[1] = "--appdir";
        all[2] = dir.toString();
        System.arraycopy(args, 0, all, 3, args.length);
        CommandLine cl = new CommandLine(new Cli.Root()).setDefaultValueProvider(new Cli.CheckoutDefaults());
        return (PublishCommand) cl.parseArgs(all).subcommand().commandSpec().userObject();
    }

    @Test
    public void spaceAfterACommaIsNotPartOfTheId() {
        assertEquals(Arrays.asList("leadman-lib", "payment-lib"),
                parse("--appids", "leadman-lib, payment-lib").appIds);
    }

    @Test
    public void aPlainListIsUnchanged() {
        assertEquals(Arrays.asList("leadman-lib", "payment-lib"),
                parse("--appids", "leadman-lib,payment-lib").appIds);
        assertEquals(Arrays.asList("*"), parse("-a", "*").appIds);
        assertEquals(Arrays.asList("/libs"), parse("-a", "/libs").appIds);
    }

    @Test
    public void repeatingTheOptionStillAccumulates() {
        assertEquals(Arrays.asList("leadman-lib", "payment-lib"),
                parse("-a", "leadman-lib", "-a", "payment-lib").appIds);
    }
}
