package co.kademi.sync;

import co.kademi.deploy.AppDeployer.FanoutBean;
import co.kademi.deploy.AppDeployerUtils;
import io.milton.common.Path;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.triplets.HashCalc;
import org.hashsplit4j.triplets.ITriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sends the objects a tree adds to the last one the server took, as zips, without asking the server what it has. */
class BulkPush {

    private static final Logger log = LoggerFactory.getLogger(BulkPush.class);

    static final Path BLOBS = Path.path("/_hashes/blobs/bulkBlobs.zip");
    static final Path CHUNKS = Path.path("/_hashes/chunkFanouts/fanouts.zip");
    static final Path FILES = Path.path("/_hashes/fileFanouts/fanouts.zip");

    interface Put {

        void put(Path path, byte[] zip) throws IOException;
    }

    int maxZipBytes = 10 << 20;
    int maxFanouts = 2000;

    private final BlobStore blobs;
    private final HashStore hashes;
    private final Put put;
    private final HashCalc hashCalc = HashCalc.getInstance();

    private final Set<String> seenBlobs = new HashSet<>();
    private final Set<String> seenChunks = new HashSet<>();
    private final Set<String> seenFiles = new HashSet<>();
    private final List<BlobImpl> blobBatch = new ArrayList<>();
    private long blobBatchBytes;
    private final List<FanoutBean> chunkFanouts = new ArrayList<>();
    private final List<FanoutBean> fileFanouts = new ArrayList<>();
    private final Set<String> missing = new LinkedHashSet<>();
    private final List<Future<?>> sending = new ArrayList<>();
    // ponytail: at most 4 zips sending and 2 waiting, about 60MB held
    private final ThreadPoolExecutor uploads = new ThreadPoolExecutor(4, 4, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.CallerRunsPolicy());

    int blobsSent;
    long bytesSent;

    BulkPush(BlobStore blobs, HashStore hashes, Put put) {
        this.blobs = blobs;
        this.hashes = hashes;
        this.put = put;
    }

    /** @param lastDirHash the root the server already has everything under, or null to send the whole tree */
    void send(String dirHash, String lastDirHash) throws IOException {
        try {
            walk(dirHash, lastDirHash);
            if (!missing.isEmpty()) {
                return;
            }
            sendBlobs();
            // Blobs, then chunk fanouts, then file fanouts: other clients take a fanout on the server to mean all below it is there
            finishSending();
            sendFanouts(CHUNKS, chunkFanouts);
            finishSending();
            sendFanouts(FILES, fileFanouts);
            finishSending();
        } finally {
            uploads.shutdownNow();
        }
    }

    /** Hashes this checkout should have and does not, so the tree cannot be sent. */
    Set<String> getMissing() {
        return missing;
    }

    int getFileCount() {
        return fileFanouts.size();
    }

    private void walk(String dirHash, String lastDirHash) throws IOException {
        if (dirHash.equals(lastDirHash) || !seenBlobs.add(dirHash)) {
            return;
        }
        byte[] listing = blobs.getBlob(dirHash);
        if (listing == null) {
            missing.add(dirHash);
            return;
        }
        addBlob(dirHash, listing);
        Map<String, ITriplet> before = new HashMap<>();
        byte[] lastListing = lastDirHash == null ? null : blobs.getBlob(lastDirHash);
        if (lastListing != null) {
            for (ITriplet t : hashCalc.parseTriplets(new ByteArrayInputStream(lastListing))) {
                before.put(t.getName(), t);
            }
        }
        for (ITriplet t : hashCalc.parseTriplets(new ByteArrayInputStream(listing))) {
            ITriplet was = before.get(t.getName());
            String wasHash = was != null && was.getType().equals(t.getType()) ? was.getHash() : null;
            if (t.getType().equals("d")) {
                walk(t.getHash(), wasHash);
            } else if (!t.getHash().equals(wasHash)) {
                addFile(t.getHash());
            }
        }
    }

    private void addFile(String fileHash) throws IOException {
        if (!seenFiles.add(fileHash)) {
            return;
        }
        Fanout file = hashes.getFileFanout(fileHash);
        if (file == null) {
            missing.add(fileHash);
            return;
        }
        for (String chunkHash : file.getHashes()) {
            if (!seenChunks.add(chunkHash)) {
                continue;
            }
            Fanout chunk = hashes.getChunkFanout(chunkHash);
            if (chunk == null) {
                missing.add(chunkHash);
                continue;
            }
            for (String blobHash : chunk.getHashes()) {
                if (seenBlobs.add(blobHash)) {
                    byte[] bytes = blobs.getBlob(blobHash);
                    if (bytes == null) {
                        missing.add(blobHash);
                    } else {
                        addBlob(blobHash, bytes);
                    }
                }
            }
            chunkFanouts.add(new FanoutBean(chunkHash, chunk.getHashes(), chunk.getActualContentLength()));
        }
        fileFanouts.add(new FanoutBean(fileHash, file.getHashes(), file.getActualContentLength()));
    }

    private void addBlob(String hash, byte[] bytes) throws IOException {
        if (!missing.isEmpty()) {
            return; // it will not be sent, so stop holding blobs
        }
        if (blobBatchBytes + bytes.length > maxZipBytes && !blobBatch.isEmpty()) {
            sendBlobs();
        }
        blobBatch.add(new BlobImpl(hash, bytes));
        blobBatchBytes += bytes.length;
    }

    private void sendBlobs() throws IOException {
        if (blobBatch.isEmpty()) {
            return;
        }
        List<BlobImpl> batch = new ArrayList<>(blobBatch);
        blobBatch.clear();
        blobsSent += batch.size();
        bytesSent += blobBatchBytes;
        blobBatchBytes = 0;
        send(BLOBS, () -> AppDeployerUtils.compressBulkBlobs(batch));
        checkSent();
    }

    private void sendFanouts(Path path, List<FanoutBean> fanouts) throws IOException {
        for (int from = 0; from < fanouts.size(); from += maxFanouts) {
            Set<FanoutBean> batch = new HashSet<>(fanouts.subList(from, Math.min(fanouts.size(), from + maxFanouts)));
            send(path, () -> AppDeployerUtils.compressBulkFanouts(batch));
        }
    }

    private interface Zip {

        byte[] zip() throws Exception;
    }

    private void send(Path path, Zip zip) {
        sending.add(uploads.submit(() -> {
            put.put(path, zip.zip());
            return null;
        }));
    }

    /** Stops at the first zip the server refused, rather than sending the rest of a push that cannot finish. */
    private void checkSent() throws IOException {
        for (Future<?> f : sending) {
            if (f.isDone()) {
                get(f);
            }
        }
        sending.removeIf(Future::isDone);
    }

    private void finishSending() throws IOException {
        for (Future<?> f : sending) {
            get(f);
        }
        sending.clear();
        log.debug("Bulk push: {} blobs, {} bytes so far", blobsSent, bytesSent);
    }

    private static void get(Future<?> f) throws IOException {
        try {
            f.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted sending objects", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            throw cause instanceof IOException ? (IOException) cause : new IOException(cause.getMessage(), cause);
        }
    }
}
