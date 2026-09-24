package co.kademi.sync;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A checkout's objects in append-only packs with one sorted index, in ksync-go's format (hashsplit/pack_store.go). */
public class PackStore implements BlobStore, HashStore, Closeable {

    private static final Logger log = LoggerFactory.getLogger(PackStore.class);

    /** Under the checkout's .ksync folder. */
    public static final String DIR = "objects";
    static final String INDEX = "index.dat";
    static final String LOCK = "lock";

    /** Separate: a one-blob file's blob, chunk fanout and file fanout share a hash. */
    static final byte NS_BLOB = 0;
    static final byte NS_CHUNK = 1;
    static final byte NS_FILE = 2;

    private static final byte[] MAGIC = "KSYNCPK1".getBytes(StandardCharsets.US_ASCII);
    static final int HEADER = 16; // magic(8) + entry count(4) + next pack id(2) + pad(2)
    /** ns, digest length, digest[32], pack id, offset, length: little endian, sorted unsigned on the first 34 bytes. */
    static final int ENTRY = 48;
    static final int KEY = 34;
    private static final int MAX_DIGEST = 32;
    static final long DEFAULT_MAX_PACK = 2L << 30;
    private static final long FLUSH_EVERY_MILLIS = 5000;

    /** Checked before the lock file is touched: closing any descriptor on it drops this process's POSIX lock. */
    private static final Set<Path> OPEN = ConcurrentHashMap.newKeySet();

    private static final boolean WINDOWS = File.separatorChar == '\\';

    private final Path dir;
    private final Path key;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final ScheduledExecutorService flusher;
    private final Thread shutdownHook;

    /** The sorted table as on disk, without its header. */
    private byte[] index;
    /** Appended since the last flush: key to {pack id, offset, length}. */
    private final Map<ByteBuffer, long[]> pending = new HashMap<>();
    /** An entry was forgotten, so the index on disk names something it should not. */
    private boolean dirty;
    private FileChannel current;
    private int currentId;
    private long currentSize;
    private int nextPackId;
    private boolean closed;

    long maxPackSize = DEFAULT_MAX_PACK;

    /** @throws SetupException when another process has the store open */
    public static PackStore open(File dir) throws IOException {
        return new PackStore(dir.toPath());
    }

    private PackStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        key = dir.toRealPath();
        if (!OPEN.add(key)) {
            throw inUse();
        }
        FileChannel ch = null;
        FileLock l = null;
        try {
            try {
                ch = FileChannel.open(dir.resolve(LOCK), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            } catch (FileSystemException ex) {
                // ksync-go on Windows opens it unshared, and a sharing violation is a plain FileSystemException
                if (WINDOWS && ex.getClass() == FileSystemException.class) {
                    throw inUse();
                }
                throw ex;
            }
            // Whole file, the same range ksync-go locks
            l = ch.tryLock();
            if (l == null) {
                throw inUse();
            }
            lockChannel = ch;
            lock = l;
            loadIndex();
        } catch (IOException | RuntimeException ex) {
            if (l != null) {
                l.release();
            }
            if (ch != null) {
                ch.close();
            }
            OPEN.remove(key);
            throw ex;
        }
        flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ksync-pack-flush");
            t.setDaemon(true);
            return t;
        });
        flusher.scheduleWithFixedDelay(this::flushQuietly, FLUSH_EVERY_MILLIS, FLUSH_EVERY_MILLIS, TimeUnit.MILLISECONDS);
        shutdownHook = new Thread(this::closeQuietly, "ksync-pack-close");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private SetupException inUse() {
        return new SetupException("Another ksync3 or ksync is already using " + dir + ". Stop it, or wait for it to finish");
    }

    private Path packPath(int id) {
        return dir.resolve(String.format("pack-%05d.dat", id));
    }

    /** Whether nothing has ever been flushed here. */
    public boolean isNew() {
        return !Files.exists(dir.resolve(INDEX));
    }

    private void loadIndex() throws IOException {
        Path p = dir.resolve(INDEX);
        if (!Files.exists(p)) {
            index = new byte[0];
            return;
        }
        byte[] data = Files.readAllBytes(p);
        byte[] table = tableOf(data);
        if (table != null) {
            index = table;
            nextPackId = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(12) & 0xffff;
            return;
        }
        // Only an outside edit damages a renamed-into-place index, and everything here can be rebuilt or refetched
        log.warn("{} is damaged, so the objects in {} are discarded and will be rebuilt or fetched as needed", p, dir);
        try (DirectoryStream<Path> packs = Files.newDirectoryStream(dir, "pack-*.dat")) {
            for (Path pack : packs) {
                Files.delete(pack);
            }
        }
        Files.delete(p);
        index = new byte[0];
    }

    /** The entries of an index file, or null when it is not one. */
    private static byte[] tableOf(byte[] data) {
        if (data.length < HEADER || !Arrays.equals(data, 0, MAGIC.length, MAGIC, 0, MAGIC.length)) {
            return null;
        }
        long count = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(8) & 0xffffffffL;
        if (data.length - HEADER != count * ENTRY) {
            return null;
        }
        return Arrays.copyOfRange(data, HEADER, data.length);
    }

    /** Null when not hex, or over 32 bytes, which truncating to fit would make collide. */
    static byte[] keyFor(byte ns, String hash) {
        if (hash == null || hash.isEmpty() || hash.length() % 2 != 0 || hash.length() / 2 > MAX_DIGEST) {
            return null;
        }
        byte[] key = new byte[KEY];
        key[0] = ns;
        key[1] = (byte) (hash.length() / 2);
        for (int i = 0; i < hash.length(); i += 2) {
            int hi = Character.digit(hash.charAt(i), 16);
            int lo = Character.digit(hash.charAt(i + 1), 16);
            if (hi < 0 || lo < 0) {
                return null;
            }
            key[2 + i / 2] = (byte) (hi << 4 | lo);
        }
        return key;
    }

    /** The byte offset of a key's entry in a sorted table, or -1. */
    static int indexAt(byte[] table, byte[] key) {
        int lo = 0;
        int hi = table.length / ENTRY;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            // Unsigned, like ksync-go's bytes.Compare
            int c = Arrays.compareUnsigned(table, mid * ENTRY, mid * ENTRY + KEY, key, 0, KEY);
            if (c < 0) {
                lo = mid + 1;
            } else if (c > 0) {
                hi = mid;
            } else {
                return mid * ENTRY;
            }
        }
        return -1;
    }

    /** @return {pack id, offset, length}, or null. Called holding the lock. */
    private long[] find(byte[] key) {
        long[] loc = pending.get(ByteBuffer.wrap(key));
        if (loc != null) {
            return loc;
        }
        int at = indexAt(index, key);
        if (at < 0) {
            return null;
        }
        ByteBuffer b = ByteBuffer.wrap(index).order(ByteOrder.LITTLE_ENDIAN);
        return new long[]{b.getShort(at + 34) & 0xffff, b.getLong(at + 36), b.getInt(at + 44) & 0xffffffffL};
    }

    private synchronized boolean has(byte ns, String hash) {
        byte[] key = keyFor(ns, hash);
        return key != null && find(key) != null;
    }

    private synchronized void put(byte ns, String hash, byte[] data) {
        if (closed) {
            throw new IllegalStateException(dir + " is closed");
        }
        byte[] key = keyFor(ns, hash);
        if (key == null) {
            throw new IllegalArgumentException("Cannot store " + hash + ": not a hex digest of 1 to " + MAX_DIGEST + " bytes");
        }
        if (find(key) != null) {
            return; // content addressed
        }
        try {
            ensurePack(data.length);
            // The file's end, not a running count, so a partial write or a crashed run's leftovers cannot shift offsets
            long offset = current.size();
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                current.write(buf);
            }
            currentSize = offset + data.length;
            pending.put(ByteBuffer.wrap(key), new long[]{currentId, offset, data.length});
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not append to " + packPath(currentId), ex);
        }
    }

    /** An object lands whole in one pack, so a pack can pass the limit by one object. */
    private void ensurePack(int incoming) throws IOException {
        if (current != null && currentSize + incoming <= maxPackSize) {
            return;
        }
        if (current != null) {
            // Synced before rolling off it, because flush syncs only the current pack
            current.force(false);
            current.close();
            current = null;
        }
        if (nextPackId > 0) {
            Path newest = packPath(nextPackId - 1);
            if (Files.exists(newest) && Files.size(newest) + incoming <= maxPackSize) {
                openForAppend(nextPackId - 1);
                return;
            }
        }
        if (nextPackId > 0xffff) {
            throw new IOException("No pack ids left in " + dir);
        }
        openForAppend(nextPackId);
        nextPackId++;
    }

    private void openForAppend(int id) throws IOException {
        current = FileChannel.open(packPath(id), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        currentId = id;
        currentSize = current.size();
    }

    private byte[] read(byte ns, String hash) {
        byte[] key = keyFor(ns, hash);
        if (key == null) {
            return null;
        }
        long[] loc;
        synchronized (this) {
            loc = find(key);
        }
        if (loc == null) {
            return null;
        }
        byte[] data = new byte[(int) loc[2]];
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (FileChannel ch = FileChannel.open(packPath((int) loc[0]), StandardOpenOption.READ)) {
            while (buf.hasRemaining()) {
                if (ch.read(buf, loc[1] + buf.position()) < 0) {
                    return forget(ns, hash, "its pack ends before it does");
                }
            }
        } catch (NoSuchFileException ex) {
            return forget(ns, hash, "its pack is missing");
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not read " + hash + " from " + packPath((int) loc[0]), ex);
        }
        return data;
    }

    /** Drops a bad copy, since put skips keys it has; the null lets a MultipleBlobStore try the next store. */
    private <T> T forget(byte ns, String hash, String why) {
        log.warn("{} in {} is damaged ({}), so it will be fetched or rebuilt", hash, dir, why);
        byte[] key = keyFor(ns, hash);
        synchronized (this) {
            if (pending.remove(ByteBuffer.wrap(key)) == null) {
                int at = indexAt(index, key);
                if (at >= 0) {
                    byte[] t = new byte[index.length - ENTRY];
                    System.arraycopy(index, 0, t, 0, at);
                    System.arraycopy(index, at + ENTRY, t, at, index.length - at - ENTRY);
                    index = t;
                }
            }
            dirty = true;
        }
        return null;
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        put(NS_BLOB, hash, bytes);
    }

    /** Checked against its name: a search and replace across the checkout rewrites packs too. */
    @Override
    public byte[] getBlob(String hash) {
        byte[] data = read(NS_BLOB, hash);
        if (data != null && !hashesTo(hash, data)) {
            return forget(NS_BLOB, hash, "it does not hash to its name");
        }
        return data;
    }

    /** A name is the Parser's double digest, sha1Hex(sha1) or the SHA-256 equivalent. */
    static boolean hashesTo(String hash, byte[] bytes) {
        switch (hash.length()) {
            case 40:
                return hash.equalsIgnoreCase(DigestUtils.sha1Hex(DigestUtils.sha1(bytes)));
            case 64:
                return hash.equalsIgnoreCase(DigestUtils.sha256Hex(DigestUtils.sha256(bytes)));
            default:
                // ponytail: nothing longer fits a key
                return false;
        }
    }

    @Override
    public boolean hasBlob(String hash) {
        return has(NS_BLOB, hash);
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        put(NS_CHUNK, hash, formatLines(blobHashes, actualContentLength));
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        put(NS_FILE, hash, formatLines(fanoutHashes, actualContentLength));
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        return fanout(NS_FILE, fileHash);
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        return fanout(NS_CHUNK, fanoutHash);
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return has(NS_CHUNK, fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return has(NS_FILE, fileHash);
    }

    private Fanout fanout(byte ns, String hash) {
        byte[] data = read(ns, hash);
        if (data == null) {
            return null;
        }
        Fanout f = parseLines(data, hash.length());
        return f != null ? f : forget(ns, hash, "it is not a fanout");
    }

    /** As FanoutSerializationUtils writes it: the length, then a newline before each hash. */
    static byte[] formatLines(List<String> hashes, long contentLength) {
        StringBuilder sb = new StringBuilder(Long.toString(contentLength));
        for (String h : hashes) {
            sb.append('\n').append(h);
        }
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    /** Null unless line one is a number and the rest are hashes as long as the name, which catches a shifted offset. */
    static Fanout parseLines(byte[] data, int hashLength) {
        String text = new String(data, StandardCharsets.US_ASCII);
        int end = text.length();
        while (end > 0 && (text.charAt(end - 1) == '\n' || text.charAt(end - 1) == '\r')) {
            end--; // trailing newlines, which a proxy can add
        }
        text = text.substring(0, end);
        int nl = text.indexOf('\n');
        long length;
        try {
            length = Long.parseLong(stripCr(nl < 0 ? text : text.substring(0, nl)));
        } catch (NumberFormatException ex) {
            return null;
        }
        List<String> hashes = new ArrayList<>();
        if (nl >= 0) {
            for (String line : text.substring(nl + 1).split("\n", -1)) {
                String h = stripCr(line);
                if (h.length() != hashLength || keyFor(NS_BLOB, h) == null) {
                    return null;
                }
                hashes.add(h);
            }
        }
        return new FanoutImpl(hashes, length);
    }

    private static String stripCr(String s) {
        return s.endsWith("\r") ? s.substring(0, s.length() - 1) : s;
    }

    /** Syncs the pack before renaming the merged index over the old one, so the index only names bytes on disk. */
    public synchronized void flush() throws IOException {
        if (current != null) {
            current.force(false);
        }
        if (pending.isEmpty() && !dirty) {
            return;
        }
        byte[][] keys = pending.keySet().stream().map(ByteBuffer::array).sorted(Arrays::compareUnsigned).toArray(byte[][]::new);
        byte[] merged = new byte[index.length + keys.length * ENTRY];
        int i = 0;
        int o = 0;
        for (byte[] k : keys) {
            while (i < index.length && Arrays.compareUnsigned(index, i, i + KEY, k, 0, KEY) < 0) {
                System.arraycopy(index, i, merged, o, ENTRY);
                i += ENTRY;
                o += ENTRY;
            }
            long[] loc = pending.get(ByteBuffer.wrap(k));
            ByteBuffer e = ByteBuffer.wrap(merged, o, ENTRY).order(ByteOrder.LITTLE_ENDIAN);
            e.put(k).putShort((short) loc[0]).putLong(loc[1]).putInt((int) loc[2]);
            o += ENTRY;
        }
        System.arraycopy(index, i, merged, o, index.length - i);

        ByteBuffer header = ByteBuffer.allocate(HEADER).order(ByteOrder.LITTLE_ENDIAN);
        header.put(MAGIC).putInt(merged.length / ENTRY).putShort((short) nextPackId).putShort((short) 0);
        header.flip();
        Path tmp = dir.resolve(INDEX + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer body = ByteBuffer.wrap(merged);
            while (header.hasRemaining() || body.hasRemaining()) {
                ch.write(new ByteBuffer[]{header, body});
            }
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(INDEX), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        index = merged;
        pending.clear();
        dirty = false;
    }

    /** Read from the index file itself, for deciding whether an old store can be deleted. */
    synchronized boolean allOnDisk(byte ns, Collection<String> hashes) throws IOException {
        if (hashes.isEmpty()) {
            return true; // and there may be no index at all yet
        }
        byte[] table = tableOf(Files.readAllBytes(dir.resolve(INDEX)));
        if (table == null) {
            return false;
        }
        for (String h : hashes) {
            byte[] key = keyFor(ns, h);
            if (key == null || indexAt(table, key) < 0) {
                return false;
            }
        }
        return true;
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (IOException | RuntimeException ex) {
            log.warn("Could not save the object index in {}, will try again: {}", dir, ex.toString());
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                flush();
            } finally {
                if (current != null) {
                    current.close();
                    current = null;
                }
                lock.release();
                lockChannel.close();
                OPEN.remove(key);
            }
        }
        flusher.shutdownNow();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ex) {
            // already shutting down, and this is that hook running
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException | RuntimeException ex) {
            log.warn("Could not save the object index in {}: {}", dir, ex.toString());
        }
    }
}
