package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Objects belong outside the checkout: both stores fan a hash out over nested directories, one
 * small file each, and the folder being worked in is the worst place to leave tens of thousands of
 * them.
 */
public class ObjectStoreDirTest {

    private Path objectsRoot;
    private File checkout;

    @Before
    public void setUp() throws Exception {
        Path tmp = Files.createTempDirectory("objectstore");
        objectsRoot = tmp.resolve("objects");
        checkout = Files.createDirectory(tmp.resolve("checkout")).toFile();
        System.setProperty(ObjectStoreDir.PATH_PROPERTY, objectsRoot.toString());
    }

    @After
    public void tearDown() {
        System.clearProperty(ObjectStoreDir.PATH_PROPERTY);
    }

    private File configDir() {
        File f = new File(checkout, ".ksync");
        f.mkdirs();
        return f;
    }

    private void writeObject(File storeDir, String name, String content) throws Exception {
        File f = new File(storeDir, "ab/cd/" + name);
        f.getParentFile().mkdirs();
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    private String read(File storeDir, String name) throws Exception {
        return new String(Files.readAllBytes(new File(storeDir, "ab/cd/" + name).toPath()),
                StandardCharsets.UTF_8);
    }

    @Test
    public void objectsAreKeptOutsideTheCheckout() {
        File dir = ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site/version1");
        assertTrue(dir.getAbsolutePath(), dir.toPath().startsWith(objectsRoot));
        assertFalse(dir.getAbsolutePath(), dir.toPath().startsWith(checkout.toPath()));
        assertTrue("created ready to write into", dir.isDirectory());
    }

    /** Content addressed, so versions of one repository can share rather than each fetching it. */
    @Test
    public void versionsOfOneRepositoryShareADirectory() {
        String repo = "https://acme.kademi.co/repositories/site";
        assertEquals(ObjectStoreDir.forRepo(repo), ObjectStoreDir.forRepo(repo));
    }

    @Test
    public void differentRepositoriesDoNot() {
        assertFalse(ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site")
                .equals(ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/other")));
    }

    @Test
    public void anExistingCheckoutsObjectsAreMovedOutOfIt() throws Exception {
        File config = configDir();
        writeObject(new File(config, ObjectStoreDir.BLOBS), "blob1", "one");
        writeObject(new File(config, ObjectStoreDir.HASHES), "fanout1", "two");

        File objects = ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site");
        ObjectStoreDir.migrate(config, objects);

        assertFalse("nothing left for an editor to index",
                new File(config, ObjectStoreDir.BLOBS).exists());
        assertFalse(new File(config, ObjectStoreDir.HASHES).exists());
        assertEquals("one", read(new File(objects, ObjectStoreDir.BLOBS), "blob1"));
        assertEquals("two", read(new File(objects, ObjectStoreDir.HASHES), "fanout1"));
    }

    /** The second checkout of a repository finds objects already there, and must not lose its own. */
    @Test
    public void aSecondCheckoutMergesIntoWhatIsAlreadyThere() throws Exception {
        File objects = ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site");
        writeObject(new File(objects, ObjectStoreDir.BLOBS), "alreadyHere", "first");

        File config = configDir();
        writeObject(new File(config, ObjectStoreDir.BLOBS), "broughtAlong", "second");
        ObjectStoreDir.migrate(config, objects);

        assertEquals("first", read(new File(objects, ObjectStoreDir.BLOBS), "alreadyHere"));
        assertEquals("second", read(new File(objects, ObjectStoreDir.BLOBS), "broughtAlong"));
        assertFalse(new File(config, ObjectStoreDir.BLOBS).exists());
    }

    @Test
    public void aCheckoutWithNothingToMove_isLeftAlone() {
        File config = configDir();
        File objects = ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site");
        ObjectStoreDir.migrate(config, objects);
        assertTrue("the checkout's own state directory stays", config.isDirectory());
        assertFalse(new File(config, ObjectStoreDir.BLOBS).exists());
    }

    /** Properties and the remote ref identify the checkout, so they stay with it. */
    @Test
    public void theCheckoutsOwnMetadataIsNotMoved() throws Exception {
        File config = configDir();
        Files.write(new File(config, "ksync.properties").toPath(),
                "url=https://acme.kademi.co/repositories/site/version1\n".getBytes(StandardCharsets.UTF_8));
        writeObject(new File(config, ObjectStoreDir.BLOBS), "blob1", "one");

        ObjectStoreDir.migrate(config, ObjectStoreDir.forRepo("https://acme.kademi.co/repositories/site"));

        assertTrue(new File(config, "ksync.properties").isFile());
    }

    @Test
    public void theDefaultRootIsACacheDirectory_notTheConfigDirectory() {
        System.clearProperty(ObjectStoreDir.PATH_PROPERTY);
        Path root = ObjectStoreDir.root();
        // on Windows the config dir is the roaming AppData, which a managed profile copies to a
        // server at logon: an object store is the last thing that should go over a network
        assertFalse(root.toString(), root.startsWith(CredentialStore.userConfigDir()));
        assertTrue(root.toString(), root.startsWith(CredentialStore.userCacheDir()));
        assertTrue(root.toString(), root.endsWith(java.nio.file.Paths.get("ksync", "objects")));
    }
}
