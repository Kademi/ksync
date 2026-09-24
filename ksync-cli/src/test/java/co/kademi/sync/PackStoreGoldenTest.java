package co.kademi.sync;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.Parser;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/** packstore-golden is ksync-go's TestPackStoreGolden output; refresh ksync-go's copy with -Dpackstore.java.out=../../ksync-go/hashsplit/testdata/packstore-java */
public class PackStoreGoldenTest {

    /** namespace, hash, content: a manifest line, with the content left off when it is empty. */
    private static List<Object[]> manifest(Path file) throws Exception {
        List<Object[]> out = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.US_ASCII)) {
            if (line.isEmpty()) {
                continue;
            }
            String[] f = line.split(" ");
            out.add(new Object[]{Byte.parseByte(f[0]), f[1], f.length > 2 ? Base64.getDecoder().decode(f[2]) : new byte[0]});
        }
        return out;
    }

    /** The exact bytes the store holds for an object, as a manifest names them. */
    private static byte[] stored(PackStore s, byte ns, String hash) {
        if (ns == PackStore.NS_BLOB) {
            return s.getBlob(hash);
        }
        Fanout f = ns == PackStore.NS_CHUNK ? s.getChunkFanout(hash) : s.getFileFanout(hash);
        return f == null ? null : PackStore.formatLines(f.getHashes(), f.getActualContentLength());
    }

    private static void assertReads(File objects, Path manifestFile) throws Exception {
        List<Object[]> want = manifest(manifestFile);
        assertTrue(want.size() > 10);
        try (PackStore s = PackStore.open(objects)) {
            for (Object[] o : want) {
                assertArrayEquals(o[1] + " in namespace " + o[0], (byte[]) o[2], stored(s, (byte) o[0], (String) o[1]));
            }
        }
    }

    @Test
    public void readsWhatKsyncGoWrote() throws Exception {
        Path golden = Paths.get("src/test/resources/packstore-golden");
        // A copy, because opening a store takes its lock file
        File objects = Files.createTempDirectory("golden").resolve(PackStore.DIR).toFile();
        FileUtils.copyDirectory(golden.resolve(PackStore.DIR).toFile(), objects);
        assertReads(objects, golden.resolve("manifest.txt"));
    }

    @Test
    public void writesAStoreForKsyncGo() throws Exception {
        String out = System.getProperty("packstore.java.out");
        Path dir = out != null ? Paths.get(out) : Files.createTempDirectory("packstore-java");
        FileUtils.deleteDirectory(dir.toFile());
        File objects = dir.resolve(PackStore.DIR).toFile();

        byte[] random = new byte[614400];
        new Random(0).nextBytes(random);
        try (PackStore s = PackStore.open(objects)) {
            s.maxPackSize = 16 << 10; // so objects spread over several packs
            for (byte[] input : Arrays.asList("hello\n".getBytes(StandardCharsets.UTF_8), new byte[0], random)) {
                new Parser().parse(new ByteArrayInputStream(input), s, s);
            }
        }
        writeManifest(objects.toPath(), dir.resolve("manifest.txt"));
        assertTrue("several packs", new File(objects, "pack-00002.dat").exists());
        assertReads(objects, dir.resolve("manifest.txt"));
    }

    /** From the index as written, in its order, with each object's bytes read from its pack. */
    private static void writeManifest(Path objects, Path manifest) throws Exception {
        byte[] index = Files.readAllBytes(objects.resolve(PackStore.INDEX));
        ByteBuffer le = ByteBuffer.wrap(index).order(ByteOrder.LITTLE_ENDIAN);
        StringBuilder sb = new StringBuilder();
        for (int at = PackStore.HEADER; at < index.length; at += PackStore.ENTRY) {
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < index[at + 1]; i++) {
                hex.append(String.format("%02x", index[at + 2 + i]));
            }
            int pack = le.getShort(at + 34) & 0xffff;
            int offset = (int) le.getLong(at + 36);
            int length = le.getInt(at + 44);
            byte[] packBytes = Files.readAllBytes(objects.resolve(String.format("pack-%05d.dat", pack)));
            byte[] content = Arrays.copyOfRange(packBytes, offset, offset + length);
            sb.append(index[at]).append(' ').append(hex);
            if (content.length > 0) {
                sb.append(' ').append(Base64.getEncoder().encodeToString(content));
            }
            sb.append('\n');
        }
        Files.write(manifest, sb.toString().getBytes(StandardCharsets.US_ASCII));
    }
}
