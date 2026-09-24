package co.kademi.sync;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.Parser;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import org.hashsplit4j.utils.FileSystem2Utils;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** The .ksync store is deleted once moved; the shared ~/.cache one never is. */
public class ObjectStoreDirTest {

    private static final String REPO = "https://acme.kademi.co/repositories/site";
    private static final byte[] CONTENT = "function loadCart() { return cart; }".getBytes(StandardCharsets.UTF_8);

    private Path objectsRoot;
    private File configDir;
    private PackStore packs;

    @Before
    public void setUp() throws Exception {
        Path tmp = Files.createTempDirectory("objectstore");
        objectsRoot = tmp.resolve("objects");
        configDir = new File(Files.createDirectory(tmp.resolve("checkout")).toFile(), ".ksync");
        configDir.mkdirs();
        System.setProperty(ObjectStoreDir.PATH_PROPERTY, objectsRoot.toString());
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(ObjectStoreDir.PATH_PROPERTY);
        if (packs != null) {
            packs.close();
        }
    }

    private PackStore packs() throws Exception {
        packs = PackStore.open(new File(configDir, PackStore.DIR));
        return packs;
    }

    /** Parses CONTENT into a loose store the way ksync3 used to, and returns its file hash. */
    private static String looseStore(File storeDir) throws Exception {
        return new Parser().parse(new ByteArrayInputStream(CONTENT),
                new FileSystem2HashStore(new File(storeDir, ObjectStoreDir.HASHES)),
                new FileSystem2BlobStore(new File(storeDir, ObjectStoreDir.BLOBS)));
    }

    private void assertHasContent(PackStore s, String fileHash) {
        Fanout file = s.getFileFanout(fileHash);
        assertEquals(CONTENT.length, file.getActualContentLength());
        Fanout chunk = s.getChunkFanout(file.getHashes().get(0));
        assertArrayEquals(CONTENT, s.getBlob(chunk.getHashes().get(0)));
    }

    @Test
    public void aLooseStoreMovesIntoPacksAndIsDeleted() throws Exception {
        String fileHash = looseStore(configDir);

        ObjectStoreDir.migrate(packs(), configDir, REPO);

        assertHasContent(packs, fileHash);
        assertFalse(new File(configDir, ObjectStoreDir.BLOBS).exists());
        assertFalse(new File(configDir, ObjectStoreDir.HASHES).exists());
    }

    @Test
    public void aDamagedLooseBlobIsLeftBehind() throws Exception {
        String damaged = "5020e412cd9884ef2511775fa9bb89110112d950";
        File f = FileSystem2Utils.toFile(new File(configDir, ObjectStoreDir.BLOBS), damaged);
        f.getParentFile().mkdirs();
        Files.write(f.toPath(), "not what that hashes to".getBytes(StandardCharsets.UTF_8));

        ObjectStoreDir.migrate(packs(), configDir, REPO);

        assertFalse(packs.hasBlob(damaged));
        assertFalse(new File(configDir, ObjectStoreDir.BLOBS).exists());
    }

    @Test
    public void theSharedStoreIsCopiedForACheckoutFromBeforePacksAndKept() throws Exception {
        File shared = objectsRoot.resolve(KSync3Utils.makeFileName(REPO)).toFile();
        String fileHash = looseStore(shared);
        Files.write(new File(configDir, "ksync.properties").toPath(), "remoteHash=abc\n".getBytes(StandardCharsets.UTF_8));

        ObjectStoreDir.migrate(packs(), configDir, REPO);

        assertHasContent(packs, fileHash);
        assertTrue("other checkouts may still read it", new File(shared, ObjectStoreDir.BLOBS).isDirectory());
    }

    @Test
    public void aNewCheckoutDoesNotCopyTheSharedStore() throws Exception {
        String fileHash = looseStore(objectsRoot.resolve(KSync3Utils.makeFileName(REPO)).toFile());

        ObjectStoreDir.migrate(packs(), configDir, REPO);

        assertFalse("nothing synced yet, so a checkout is about to fetch it all", packs.hasFile(fileHash));
    }

    @Test
    public void bothIgnoreFilesAreWrittenAndAnEditedOneIsLeftAlone() throws Exception {
        Files.write(new File(configDir, ".gitignore").toPath(), "blobs/\n".getBytes(StandardCharsets.UTF_8));

        ObjectStoreDir.hideFromTools(configDir);

        assertEquals("blobs/\n", new String(Files.readAllBytes(new File(configDir, ".gitignore").toPath()), StandardCharsets.UTF_8));
        String ignore = new String(Files.readAllBytes(new File(configDir, ".ignore").toPath()), StandardCharsets.UTF_8);
        assertTrue(ignore, ignore.endsWith("\n*\n"));
    }
}
