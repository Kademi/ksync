package io.milton.sync.triplets;

import co.kademi.sync.GlobalIgnores;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchKey;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * An ignored directory must cost no watches.
 *
 * This is a silent failure if it regresses: linux caps inotify watches per user, node_modules can
 * hold tens of thousands of directories, and once the cap is hit the files that do matter stop
 * being watched with nothing to say so.
 */
public class FileSystemWatchingServiceIgnoresTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private List<WatchKey> watch(File root, List<String> ignores) throws IOException {
        FileSystemWatchingService svc = new FileSystemWatchingService(
                FileSystems.getDefault().newWatchService(), null);
        return svc.watch(root, (event, changed) -> {
        }, ignores);
    }

    @Test
    public void anIgnoredDirectoryIsNotWatchedAndIsNotDescendedInto() throws Exception {
        File root = tmp.getRoot();
        new File(root, "theme").mkdirs();
        // three levels deep, which is what makes the difference big in a real node_modules
        new File(root, "node_modules/leftpad/dist").mkdirs();
        new File(root, "node_modules/other").mkdirs();

        List<WatchKey> withIgnores = watch(root, GlobalIgnores.BUILT_IN);
        assertEquals("only the root and theme should be watched", 2, withIgnores.size());

        // and without them, to show the test is measuring the thing it claims to
        List<WatchKey> withoutIgnores = watch(root, null);
        assertTrue("unignored, node_modules should add watches: " + withoutIgnores.size(),
                withoutIgnores.size() > withIgnores.size());
    }

    @Test
    public void localIgnoresAreHonouredTooNotJustTheBuiltIns() throws Exception {
        File root = tmp.getRoot();
        new File(root, "keep").mkdirs();
        new File(root, "huge-generated-thing").mkdirs();

        assertEquals(2, watch(root, Arrays.asList("huge-generated-thing")).size());
    }
}
