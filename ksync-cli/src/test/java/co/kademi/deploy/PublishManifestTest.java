package co.kademi.deploy;

import co.kademi.sync.SetupException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The refusals from ksync-go's publish/manifest_test.go, since both tools read one file. */
public class PublishManifestTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    static final String MARKETPLACE = "concurrency = 4\n\n"
            + "[[tiers]]\ndir   = \"libs\"\ntype  = \"lib\"\nfirst = [\"admin-lib\"]   # must be live first, or things break\n\n"
            + "[[tiers]]\ndir  = \"apps\"\ntype = \"app\"\n\n"
            + "[[tiers]]\ndir  = \"themes\"\ntype = \"theme\"\n\n"
            + "[[tiers]]\ndir  = \"recipes\"\ntype = \"recipe\"\n";

    private File write(String content) throws Exception {
        File dir = tmp.newFolder();
        Files.write(new File(dir, PublishManifest.FILE).toPath(), content.getBytes(StandardCharsets.UTF_8));
        return dir;
    }

    private String refusal(String content) throws Exception {
        try {
            PublishManifest.load(write(content));
        } catch (SetupException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith(PublishManifest.FILE + ": "));
            return ex.getMessage();
        }
        fail("accepted:\n" + content);
        return null;
    }

    @Test
    public void theMarketplaceManifestLoadsInOrder() throws Exception {
        PublishManifest m = PublishManifest.load(write(MARKETPLACE));
        List<String> dirs = new ArrayList<>();
        for (PublishManifest.Tier t : m.tiers) {
            dirs.add(t.dir + ":" + t.type);
        }
        assertEquals(Arrays.asList("libs:lib", "apps:app", "themes:theme", "recipes:recipe"), dirs);
        assertEquals(Collections.singletonList("admin-lib"), m.tiers.get(0).first);
        assertTrue(m.tiers.get(1).first.isEmpty());
    }

    @Test
    public void noFileKeepsTheBuiltInOrder() throws Exception {
        assertNull(PublishManifest.load(tmp.newFolder()));
    }

    @Test
    public void concurrencyIsOptional() throws Exception {
        assertEquals(1, PublishManifest.load(write("[[tiers]]\ndir = \"apps\"\ntype = \"app\"\n")).tiers.size());
    }

    @Test
    public void refusesWhatKsyncGoRefuses() throws Exception {
        String[][] cases = {
            {"[[tiers]]\ndir = \"libs\"\ntype = \"lib\"\nfrist = [\"admin-lib\"]\n", "unknown key: tiers.frist (line 4)"},
            {"wat = 1\n[[tiers]]\ndir = \"libs\"\ntype = \"lib\"\nnope = 2\n", "unknown keys: tiers.nope (line 5), wat (line 1)"},
            {"concurrency = 2\n", "no tiers"},
            {"", "no tiers"},
            {"[[tiers]]\ntype = \"app\"\n", "tier 1: no dir"},
            {"[[tiers]]\ndir = \"apps\"\n", "\"apps\" has no type"},
            {"[[tiers]]\ndir = \"apps\"\ntype = \"application\"\n", "type \"application\"; expected one of app, lib, theme, recipe"},
            {"[[tiers]]\ndir = \"a/b\"\ntype = \"app\"\n", "must be a single directory name"},
            {"[[tiers]]\ndir = \"..\"\ntype = \"app\"\n", "must be a single directory name"},
            {"[[tiers]]\ndir = \"apps\"\ntype = \"app\"\n[[tiers]]\ndir = \"apps\"\ntype = \"lib\"\n", "tier 2: \"apps\" is already tier 1"},
            {"[[tiers]]\ndir = \"libs\"\ntype = \"lib\"\nfirst = [\" \"]\n", "blank name in first"},
            {"[[tiers]]\ndir = \"libs\"\ntype = \"lib\"\nfirst = [\"a\", \"a\"]\n", "lists \"a\" twice in first"},
            {"concurrency = -1\n[[tiers]]\ndir = \"apps\"\ntype = \"app\"\n", "concurrency is -1"},
            {"this is not toml\n", "line 1"},
            {"tiers = \"apps\"\n", "tiers must be [[tiers]] tables"},
            {"[[tiers]]\ndir = 5\ntype = \"app\"\n", "dir must be a string"},
            {"[[tiers]]\ndir = \"libs\"\ntype = \"lib\"\nfirst = \"admin-lib\"\n", "first that is not a list of names"},};
        for (String[] c : cases) {
            String message = refusal(c[0]);
            assertTrue(message + " should mention " + c[1], message.contains(c[1]));
        }
    }

    @Test
    public void firstGoesFrontAndAMissingOneIsReported() {
        List<String> missing = new ArrayList<>();
        List<String> ordered = PublishManifest.order(Arrays.asList("admin-lib", "gone-lib"), Arrays.asList("a-lib", "admin-lib", "z-lib"), missing);
        assertEquals(Arrays.asList("admin-lib", "a-lib", "z-lib"), ordered);
        assertEquals(Collections.singletonList("gone-lib"), missing);
    }
}
