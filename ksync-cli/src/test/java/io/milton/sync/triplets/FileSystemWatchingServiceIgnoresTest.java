package io.milton.sync.triplets;

import co.kademi.sync.GlobalIgnores;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchKey;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    /**
     * The startup walk is only half of it: an npm install while the sync is running creates
     * node_modules under a watched root, and the create event used to register a watch on it, and
     * then on every directory inside it as each arrived.
     *
     * Events from one inotify instance arrive in the order they happened, so once the marker
     * created after the file inside node_modules has been seen, that file's event would have been
     * seen too had its directory been watched.
     */
    @Test
    public void aDirectoryCreatedWhileWatchingIsNotWatchedIfIgnored() throws Exception {
        assertFalse(seesFileCreatedInside("node_modules", GlobalIgnores.BUILT_IN));
        // and the control, so the test is measuring the thing it claims to
        assertTrue(seesFileCreatedInside("packages", GlobalIgnores.BUILT_IN));
    }

    private boolean seesFileCreatedInside(String dirName, List<String> ignores) throws Exception {
        File root = tmp.newFolder();
        Set<String> seen = ConcurrentHashMap.newKeySet();
        FileSystemWatchingService svc = new FileSystemWatchingService(
                FileSystems.getDefault().newWatchService(), null);
        svc.watch(root, (event, changed) -> seen.add(changed.getName()), ignores);

        File dir = new File(root, dirName);
        dir.mkdirs();
        pumpUntil(svc, seen, dirName);

        new File(dir, "inside.js").createNewFile();
        File marker = new File(root, "marker.txt");
        marker.createNewFile();
        pumpUntil(svc, seen, marker.getName());
        return seen.contains("inside.js");
    }

    private void pumpUntil(FileSystemWatchingService svc, Set<String> seen, String name) throws Exception {
        long deadline = System.currentTimeMillis() + 15000;
        while (!seen.contains(name)) {
            assertTrue("timed out waiting for an event for " + name, System.currentTimeMillis() < deadline);
            svc.scanFsEvents();
            Thread.sleep(20);
        }
    }
}
