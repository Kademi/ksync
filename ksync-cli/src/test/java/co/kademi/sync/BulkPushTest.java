package co.kademi.sync;

import io.milton.common.Path;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.hashsplit4j.api.Parser;
import org.hashsplit4j.store.MemoryBlobStore;
import org.hashsplit4j.store.MemoryHashStore;
import org.hashsplit4j.triplets.HashCalc;
import org.hashsplit4j.triplets.Triplet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class BulkPushTest {

    private final MemoryBlobStore blobs = new MemoryBlobStore();
    private final MemoryHashStore hashes = new MemoryHashStore();
    /** Each zip as it was put: its path, then the names in it. */
    private final List<Object[]> puts = Collections.synchronizedList(new ArrayList<>());

    private String file(String content) throws Exception {
        return new Parser().parse(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), hashes, blobs);
    }

    private static Triplet entry(String name, String hash, String type) {
        Triplet t = new Triplet();
        t.setName(name);
        t.setHash(hash);
        t.setType(type);
        return t;
    }

    private String dir(Triplet... entries) throws Exception {
        List<Triplet> list = new ArrayList<>(Arrays.asList(entries));
        HashCalc.getInstance().sort(list);
        ByteArrayOutputStream listing = new ByteArrayOutputStream();
        String hash = HashCalc.getInstance().calcHash(list, listing);
        blobs.setBlob(hash, listing.toByteArray());
        return hash;
    }

    private BulkPush bulk() {
        return new BulkPush(blobs, hashes, (path, zip) -> {
            Set<String> names = new HashSet<>();
            try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip))) {
                for (ZipEntry e; (e = in.getNextEntry()) != null;) {
                    names.add(e.getName());
                }
            }
            puts.add(new Object[]{path, names});
        });
    }

    @SuppressWarnings("unchecked")
    private Set<String> sentTo(Path path) {
        Set<String> all = new HashSet<>();
        for (Object[] p : puts) {
            if (p[0].equals(path)) {
                all.addAll((Set<String>) p[1]);
            }
        }
        return all;
    }

    @Test
    public void sendsOnlyWhatChangedSinceTheLastPush() throws Exception {
        String one = file("one");
        String two = file("two");
        String sub = dir(entry("b.txt", two, "f"));
        String before = dir(entry("a.txt", one, "f"), entry("sub", sub, "d"));

        String changed = file("one, edited");
        String three = file("three");
        String added = dir(entry("c.txt", three, "f"));
        String after = dir(entry("a.txt", changed, "f"), entry("sub", sub, "d"), entry("new", added, "d"));

        BulkPush b = bulk();
        b.send(after, before);

        assertTrue(b.getMissing().isEmpty());
        assertEquals(new HashSet<>(Arrays.asList(changed, three)), sentTo(BulkPush.FILES));
        Set<String> sentBlobs = sentTo(BulkPush.BLOBS);
        assertTrue(sentBlobs.containsAll(Arrays.asList(after, added)));
        assertFalse("an unchanged directory is not walked", sentBlobs.contains(sub));
        assertFalse(sentBlobs.contains(hashes.getChunkFanout(two).getHashes().get(0)));
    }

    @Test
    public void withNoLastPushEverythingGoesInOrder() throws Exception {
        String sub = dir(entry("b.txt", file("two"), "f"));
        String root = dir(entry("a.txt", file("one"), "f"), entry("sub", sub, "d"));

        BulkPush b = bulk();
        b.maxZipBytes = 1; // a zip per blob
        b.send(root, null);

        assertEquals(2, sentTo(BulkPush.FILES).size());
        assertTrue(sentTo(BulkPush.BLOBS).containsAll(Arrays.asList(root, sub)));
        assertTrue("split into several zips", puts.stream().filter(p -> p[0].equals(BulkPush.BLOBS)).count() >= 4);
        int lastBlob = -1;
        int firstChunk = Integer.MAX_VALUE;
        int lastChunk = -1;
        int firstFile = Integer.MAX_VALUE;
        for (int i = 0; i < puts.size(); i++) {
            Object path = puts.get(i)[0];
            if (path.equals(BulkPush.BLOBS)) {
                lastBlob = i;
            } else if (path.equals(BulkPush.CHUNKS)) {
                firstChunk = Math.min(firstChunk, i);
                lastChunk = i;
            } else {
                firstFile = Math.min(firstFile, i);
            }
        }
        assertTrue("blobs before chunk fanouts", lastBlob < firstChunk);
        assertTrue("chunk fanouts before file fanouts", lastChunk < firstFile);
    }

    @Test
    public void aTreeMissingPartOfItselfSendsNoFanouts() throws Exception {
        String root = dir(entry("a.txt", file("one"), "f"), entry("gone.txt", "ab" + String.join("", Collections.nCopies(38, "0")), "f"));

        BulkPush b = bulk();
        b.send(root, null);

        assertEquals(1, b.getMissing().size());
        assertTrue(sentTo(BulkPush.CHUNKS).isEmpty());
        assertTrue(sentTo(BulkPush.FILES).isEmpty());
    }
}
