package co.kademi.sync;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A missing url usually means the command was run outside a checkout, which picocli's own wording
 * does not say. Saying so must not swallow the other options that are missing too, or the run that
 * supplies the url fails again on the next one.
 */
public class MissingOptionsTest {

    private final PrintStream realErr = System.err;
    private ByteArrayOutputStream captured;
    private Path notACheckout;

    @Before
    public void setUp() throws Exception {
        captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        notACheckout = Files.createTempDirectory("missingoptions");
    }

    @After
    public void tearDown() {
        System.setErr(realErr);
    }

    private String errFrom(String... args) {
        String[] all = new String[args.length + 2];
        System.arraycopy(args, 0, all, 0, args.length);
        all[args.length] = "--appdir";
        all[args.length + 1] = notACheckout.toString();
        assertEquals("invalid input", 2, Cli.run(all));
        System.err.flush();
        return new String(captured.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    public void outsideACheckout_theUrlIsExplained() {
        String err = errFrom("pull");
        assertTrue(err, err.contains("is not a ksync checkout"));
    }

    @Test
    public void theOtherRequiredOptions_areNamedToo() {
        // publish needs --appids as well, and the checkout would never have supplied that
        String err = errFrom("publish");
        assertTrue(err, err.contains("is not a ksync checkout"));
        assertTrue(err, err.contains("--appids"));
    }

    @Test
    public void logout_saysWhatItCouldNotLogOutOf() {
        String err = errFrom("logout");
        assertTrue(err, err.contains("no site to log out of"));
        assertFalse(err, err.contains("Also missing"));
    }
}
