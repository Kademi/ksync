package co.kademi.sync;

import io.milton.common.Path;
import io.milton.httpclient.Host;
import io.milton.http.exceptions.NotFoundException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutSerializationUtils;
import org.hashsplit4j.api.HashStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads a whole repository's objects as a single zip and writes them into the local stores.
 * <p>
 * The alternative, walking the object graph over HTTP, costs at least four requests per file - a fanout lookup, a chunk
 * fanout lookup, and a blob GET per chunk - and every one of those re-enters the server's resource factory, security
 * manager and a fresh database session. A pack replaces all of it with one request.
 * <p>
 * The server writes children before parents, so an object only ever appears after everything it points at. That means a
 * truncated pack leaves the local stores consistent rather than corrupt, and the presence of the root hash at the end
 * proves the download completed. A pack from a server which does not support them 404s, which the caller treats as a
 * signal to fall back to the old per-object walk.
 *
 * @author brad
 */
public class PackFetcher {

    private static final Logger log = LoggerFactory.getLogger(PackFetcher.class);

    public static final String PACKS_BASE_PATH = "/_hashes/packs/";

    private static final String PREFIX_BLOB = "b/";
    private static final String PREFIX_CHUNK_FANOUT = "cf/";
    private static final String PREFIX_FILE_FANOUT = "ff/";
    private static final String ENTRY_MANIFEST = "manifest.properties";
    private static final String ENTRY_MISSING = "missing.txt";

    /**
     * How often to log progress, in objects. Packs have no content length, so this is the only feedback a user gets
     * during a large checkout.
     */
    private static final int PROGRESS_INTERVAL = 500;

    /**
     * How many times to ask for the pack before giving up and walking the objects one at a time. Each retry resumes
     * where the last one stopped rather than starting over.
     */
    private static final int MAX_ATTEMPTS = 3;

    private final Host client;
    private final BlobStore localBlobStore;
    private final HashStore localHashStore;

    private final List<String> errors = new ArrayList<>();

    private long objectCount;
    private long byteCount;

    public PackFetcher(Host client, BlobStore localBlobStore, HashStore localHashStore) {
        this.client = client;
        this.localBlobStore = localBlobStore;
        this.localHashStore = localHashStore;
    }

    /**
     * Downloads every object under the given root hash into the local stores.
     *
     * @param rootHash the repository root hash to fetch
     * @return true if the pack was downloaded and is complete, false if the server does not support packs or the pack
     * did not arrive intact, in which case the caller should fall back to fetching objects individually
     */
    public boolean fetch(String rootHash) {
        if (rootHash == null || rootHash.isEmpty() || "null".equals(rootHash)) {
            log.debug("fetchPack: no root hash, nothing to fetch");
            return true; // empty repository, nothing to do and nothing to fall back to
        }
        // doGet takes a full url rather than a path, the same way Host.get(Path) builds one before calling through
        String baseUrl = client.buildEncodedUrl(Path.path(PACKS_BASE_PATH + rootHash + ".zip"));
        long tm = System.currentTimeMillis();

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            // Resume where the last attempt stopped. Objects the server could not supply take a position in its walk
            // without reaching us, so our count is never ahead of the server's - at worst we are sent a few objects
            // again, which is harmless because writing them is idempotent.
            long skip = objectCount;
            String url = skip > 0 ? baseUrl + "?skip=" + skip : baseUrl;
            log.debug("Downloading repository objects from {}", url);
            try {
                client.doGet(url, (InputStream in) -> {
                    readPack(in);
                }, null, null);
            } catch (NotFoundException e) {
                log.debug("This server does not support pack downloads, falling back to fetching objects one at a time");
                return false;
            } catch (Exception e) {
                log.warn("Pack download failed after {} objects: {}", objectCount, e.getMessage(), e);
            }

            // The root blob is written last, so having it means we read the stream through to the end
            if (localBlobStore.hasBlob(rootHash)) {
                log.debug("Downloaded {} objects, {} bytes in {}ms", objectCount, byteCount, System.currentTimeMillis() - tm);
                return true;
            }

            if (objectCount <= skip) {
                log.warn("Pack download made no progress, falling back to fetching objects one at a time");
                return false;
            }
            log.debug("Pack download was incomplete, resuming from object {}", objectCount);
        }

        log.warn("Gave up on the pack after {} attempts, falling back to fetching objects one at a time", MAX_ATTEMPTS);
        return false;
    }

    /**
     * Objects the server reported it could not include in the pack. Not empty means the repository on the server is
     * missing content, which the caller should surface rather than silently write an incomplete working copy.
     *
     * @return the reported errors, in server order
     */
    public List<String> getErrors() {
        return errors;
    }

    private void readPack(InputStream in) throws IOException {
        ZipInputStream zin = new ZipInputStream(new BufferedInputStream(in));
        ZipEntry entry;
        while ((entry = zin.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                continue;
            }
            String name = entry.getName();
            byte[] data = IOUtils.toByteArray(zin); // reads to the end of this entry only
            byteCount += data.length;
            if (name.startsWith(PREFIX_BLOB)) {
                localBlobStore.setBlob(name.substring(PREFIX_BLOB.length()), data);
            } else if (name.startsWith(PREFIX_CHUNK_FANOUT)) {
                Fanout f = readFanout(data);
                localHashStore.setChunkFanout(name.substring(PREFIX_CHUNK_FANOUT.length()), f.getHashes(), f.getActualContentLength());
            } else if (name.startsWith(PREFIX_FILE_FANOUT)) {
                Fanout f = readFanout(data);
                localHashStore.setFileFanout(name.substring(PREFIX_FILE_FANOUT.length()), f.getHashes(), f.getActualContentLength());
            } else if (ENTRY_MANIFEST.equals(name)) {
                log.debug("Pack manifest: {}", new String(data, "UTF-8").trim().replace("\n", ", "));
                continue; // not an object
            } else if (ENTRY_MISSING.equals(name)) {
                for (String line : new String(data, "UTF-8").split("\n")) {
                    if (!line.trim().isEmpty()) {
                        errors.add("Server could not provide " + line.trim());
                    }
                }
                continue; // not an object
            } else {
                log.warn("Ignoring unrecognised pack entry {}", name);
                continue;
            }
            objectCount++;
            if (objectCount % PROGRESS_INTERVAL == 0) {
                log.debug("..received {} objects, {} bytes", objectCount, byteCount);
            }
        }
    }

    private Fanout readFanout(byte[] data) throws IOException {
        return FanoutSerializationUtils.readFanout(new ByteArrayInputStream(data));
    }
}
