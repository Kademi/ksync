package co.kademi.sync;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.bouncycastle.crypto.Digest;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.Parser;
import org.hashsplit4j.store.MemoryHashStore;
import io.milton.sync.triplets.SyncHashCache;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PackStoreTest {

    private File dir;
    private PackStore store;

    @Before
    public void setUp() throws Exception {
        dir = new File(Files.createTempDirectory("packs").toFile(), PackStore.DIR);
        store = PackStore.open(dir);
    }

    @After
    public void tearDown() throws Exception {
        store.close();
    }

    private PackStore reopen() throws Exception {
        store.close();
        store = PackStore.open(dir);
        return store;
    }

    /** Named the way the Parser names a blob. */
    private static String hashOf(byte[] bytes) {
        Digest d = Parser.getCrypt();
        d.update(bytes, 0, bytes.length);
        return Parser.toHex(d);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Path pack(int id) {
        return dir.toPath().resolve(String.format("pack-%05d.dat", id));
    }

    @Test
    public void everythingStoredIsThereAfterReopening() throws Exception {
        byte[] small = utf8("function loadCart() {}");
        String h = hashOf(small);
        // A file that is one blob: its blob, chunk fanout and file fanout share one hash
        store.setBlob(h, small);
        store.setChunkFanout(h, Collections.singletonList(h), small.length);
        store.setFileFanout(h, Collections.singletonList(h), small.length);
        String empty = hashOf(new byte[0]);
        store.setFileFanout(empty, Collections.emptyList(), 0);

        reopen();

        assertArrayEquals(small, store.getBlob(h));
        assertEquals(Collections.singletonList(h), store.getChunkFanout(h).getHashes());
        assertEquals(small.length, store.getFileFanout(h).getActualContentLength());
        Fanout none = store.getFileFanout(empty);
        assertEquals(0, none.getActualContentLength());
        assertTrue(none.getHashes().isEmpty());
        assertFalse("namespaces are separate", store.hasChunk(empty));
    }

    @Test
    public void fanoutsAreTheWireText() {
        assertEquals("12\naa\nbb", new String(PackStore.formatLines(Arrays.asList("aa", "bb"), 12), StandardCharsets.US_ASCII));
        assertEquals("0", new String(PackStore.formatLines(Collections.emptyList(), 0), StandardCharsets.US_ASCII));
        assertEquals(12, PackStore.parseLines(utf8("12\naa\nbb"), 2).getActualContentLength());
        assertNull("a shifted offset lands a hash where the length should be", PackStore.parseLines(utf8("aa\n12"), 2));
        assertNull("or runs the last hash into the next object", PackStore.parseLines(utf8("2\naa\nbbXY"), 2));
    }

    @Test
    public void theIndexIsKsyncGosLayout() throws Exception {
        byte[] b = utf8("x");
        String h = hashOf(b);
        store.setBlob(h, b);
        store.flush();

        byte[] index = Files.readAllBytes(dir.toPath().resolve(PackStore.INDEX));
        assertEquals(PackStore.HEADER + PackStore.ENTRY, index.length);
        assertEquals("KSYNCPK1", new String(index, 0, 8, StandardCharsets.US_ASCII));
        ByteBuffer le = ByteBuffer.wrap(index).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(1, le.getInt(8));
        assertEquals(1, le.getShort(12));
        assertEquals(0, le.getShort(14));
        int e = PackStore.HEADER;
        assertEquals(PackStore.NS_BLOB, index[e]);
        assertEquals(20, index[e + 1]);
        assertEquals(h.substring(0, 2), String.format("%02x", index[e + 2]));
        assertEquals(0, le.getShort(e + 34));
        assertEquals(0, le.getLong(e + 36));
        assertEquals(1, le.getInt(e + 44));
        assertArrayEquals(b, Files.readAllBytes(pack(0)));
    }

    /** Java's bytes are signed; ksync-go's order is not, and lookups must agree with it. */
    @Test
    public void entriesSortAsUnsignedBytes() throws Exception {
        String high = "ff00000000000000000000000000000000000000";
        String low = "0100000000000000000000000000000000000000";
        store.setFileFanout(high, Collections.emptyList(), 1);
        store.setFileFanout(low, Collections.emptyList(), 2);
        reopen();

        byte[] index = Files.readAllBytes(dir.toPath().resolve(PackStore.INDEX));
        assertEquals(0x01, index[PackStore.HEADER + 2] & 0xff);
        assertEquals(0xff, index[PackStore.HEADER + PackStore.ENTRY + 2] & 0xff);
        assertEquals(1, store.getFileFanout(high).getActualContentLength());
        assertEquals(2, store.getFileFanout(low).getActualContentLength());
    }

    @Test
    public void nothingIsInTheIndexUntilAFlush() throws Exception {
        byte[] b = utf8("x");
        store.setBlob(hashOf(b), b);
        assertTrue("readable before the flush", store.hasBlob(hashOf(b)));
        assertFalse(Files.exists(dir.toPath().resolve(PackStore.INDEX)));
    }

    @Test
    public void aSecondOpenIsRefused() throws Exception {
        try {
            PackStore.open(dir).close();
            fail("two writers would lose each other's entries");
        } catch (SetupException expected) {
        }
    }

    /** A search and replace across a checkout: one blob edited in place, and everything after it shifted. */
    @Test
    public void anEditedPackIsCaughtAndHealed() throws Exception {
        byte[] a = utf8("function loadCart() {}");
        byte[] b = utf8("loadCart(); render();");
        store.setBlob(hashOf(a), a);
        store.setBlob(hashOf(b), b);
        store.setChunkFanout(hashOf(a), Collections.singletonList(hashOf(a)), a.length);
        reopen();

        String text = new String(Files.readAllBytes(pack(0)), StandardCharsets.UTF_8);
        Files.write(pack(0), utf8(text.replace("loadCart", "loadShoppingCart")));

        assertNull(store.getBlob(hashOf(a)));
        assertNull(store.getBlob(hashOf(b)));
        assertNull("shifted off its offset", store.getChunkFanout(hashOf(a)));
        assertFalse("forgotten, so a good copy can be stored", store.hasBlob(hashOf(a)));

        store.setBlob(hashOf(a), a);
        reopen();
        assertArrayEquals(a, store.getBlob(hashOf(a)));
        assertFalse("the forget was saved", store.hasBlob(hashOf(b)));
    }

    @Test
    public void packsRollAndTheNewestIsReusedWhileItHasRoom() throws Exception {
        store.maxPackSize = 10;
        byte[] six = utf8("sixsix");
        byte[] five = utf8("fivef");
        byte[] one = utf8("1");
        store.setBlob(hashOf(six), six);
        store.setBlob(hashOf(five), five); // 11 would go past 10, so a new pack
        reopen();
        store.maxPackSize = 10;
        store.setBlob(hashOf(one), one); // fits in the newest

        assertEquals(6, Files.size(pack(0)));
        assertEquals(6, Files.size(pack(1)));
        assertFalse(Files.exists(pack(2)));
        assertArrayEquals(six, store.getBlob(hashOf(six)));
        assertArrayEquals(five, store.getBlob(hashOf(five)));
        assertArrayEquals(one, store.getBlob(hashOf(one)));
    }

    @Test
    public void bytesACrashLeftBehindDoNotMoveLaterOffsets() throws Exception {
        store.close();
        Files.write(pack(0), utf8("junk from a run that never flushed"));
        store = PackStore.open(dir);
        byte[] b = utf8("x");
        store.setBlob(hashOf(b), b);
        reopen();
        assertArrayEquals(b, store.getBlob(hashOf(b)));
    }

    @Test
    public void aDamagedIndexStartsOver() throws Exception {
        byte[] b = utf8("x");
        store.setBlob(hashOf(b), b);
        store.close();
        Files.write(dir.toPath().resolve(PackStore.INDEX), utf8("KSYNCPK1 edited by sed"));

        store = PackStore.open(dir);
        assertFalse(store.hasBlob(hashOf(b)));
        store.setBlob(hashOf(b), b);
        reopen();
        assertArrayEquals(b, store.getBlob(hashOf(b)));
    }

    @Test
    public void aDigestOver32BytesIsRefused() {
        String sha512 = String.join("", Collections.nCopies(128, "a"));
        try {
            store.setBlob(sha512, new byte[0]);
            fail("truncating it would collide");
        } catch (IllegalArgumentException expected) {
        }
        assertFalse(store.hasBlob(sha512));
    }

    /** A file whose objects are gone is parsed again, not taken from the cache. */
    @Test
    public void aCachedHashCountsOnlyWhileItsObjectsAreStored() {
        MemoryHashStore hashes = new MemoryHashStore();
        SyncHashCache cache = new SyncHashCache() {
            @Override
            public String get(File file) {
                return "abcd";
            }

            @Override
            public void put(File file, String hash) {
            }
        };
        SyncHashCache checked = KSync3.storedOnly(cache, hashes);
        assertNull(checked.get(new File("a")));
        hashes.setFileFanout("abcd", Collections.emptyList(), 0);
        assertEquals("abcd", checked.get(new File("a")));
    }
}
