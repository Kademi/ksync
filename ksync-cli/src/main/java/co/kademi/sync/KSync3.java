package co.kademi.sync;

import co.kademi.deploy.AppDeployer;
import io.milton.common.Path;
import io.milton.event.EventManager;
import io.milton.event.EventManagerImpl;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.triplets.DeltaGenerator;
import io.milton.sync.triplets.FileUpdatingMergingDeltaListener;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Combiner;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import org.hashsplit4j.store.MultipleBlobStore;
import org.hashsplit4j.store.MultipleHashStore;
import org.hashsplit4j.triplets.HashCalc;
import org.hashsplit4j.triplets.ITriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * checkout http://localhost:8080/repositories/w1/version1/ admin password8
 *
 * @author brad
 */
public class KSync3 {

    private static final Logger log = LoggerFactory.getLogger(KSync3.class);

    private static final List<Command> commands = new ArrayList<>();

    static {
        commands.add(new UsageCommand());
        commands.add(new CheckoutCommand());
        commands.add(new PushCommand());
        commands.add(new PullCommand());
        commands.add(new SyncCommand());
        commands.add(new PublishCommand());
    }

    public static void main(String[] arg) throws IOException {

        String commandsSt = "";
        for (Command c : commands) {
            commandsSt += c.getName() + ",";
        }

        Options options = new Options();
        options.addOption("command", true, "One of " + commandsSt);
        options.addOption("rootdir", true, "Root directory, which will contain folders 'apps', 'libs' and 'themes', each of which should contain the app folder to publish ");
        options.addOption("url", true, "URL to use, for checkout and publish");
        options.addOption("user", true, "username to use, for checkout and publish. Not your email address");
        options.addOption("password", true, "password to login with, for checkout and publish. Not your email address. Will prompt if needed and not provided");
        options.addOption("report", false, "Display report only, do not make changes (for publish command only)");
        options.addOption("versionincrement", false, "Update version files (for publish command only)");
        options.addOption("force", false, "Update already published apps (for publish command only)");
        options.addOption("appids", true, "Which apps to publish. Asterisk to load all apps; or enter a comma seperated list of ids; or absolute paths, eg * ; or /libs; or leadman-lib, payment-lib");

        CommandLineParser parser = new DefaultParser();
        CommandLine line;
        try {
            // parse the command line arguments
            line = parser.parse(options, arg);
        } catch (Exception exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            return;
        }
        
        Command cmd = KSync3Utils.findCommand(line, commands);

        if (cmd == null) {
            System.out.println("Please enter a command - " + commandsSt);
            return;
        }

        cmd.execute(options, line);

    }

    public interface Command {

        String getName();

        void execute(Options options, CommandLine line);
    }

    public static class CheckoutCommand implements Command {

        @Override
        public String getName() {
            return "checkout";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            checkout(options, line);
        }

    }

    public static class UsageCommand implements Command {

        @Override
        public String getName() {
            return "usage";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            showUsage(options);
        }

    }

    public static class CommitCommand implements Command {

        @Override
        public String getName() {
            return "commit";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            commit(options);
        }

    }

    public static class PullCommand implements Command {

        @Override
        public String getName() {
            return "pull";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            pull(options);
        }

    }

    public static class PushCommand implements Command {

        @Override
        public String getName() {
            return "push";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            push(options);
        }

    }

    public static class SyncCommand implements Command {

        @Override
        public String getName() {
            return "sync";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            sync(options);
            boolean done = false;
            while (!done) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException interruptedException) {
                    done = true;
                }
            }
            System.exit(0); // threads arent shutting down
        }

    }

    public static class PublishCommand implements Command {

        @Override
        public String getName() {
            return "publish";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            try {
                AppDeployer.publish(options, line);
            } catch (IOException ex) {
                log.error("Invalie URL", ex);
            }
        }

    }

    public static void showUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ksync3", options);
    }

    private static void checkout(Options options, CommandLine line) {
        KSyncUtils.withDir((File dir) -> {
            String url = KSync3Utils.getInput(options, line, "url", null);
            String user = KSync3Utils.getInput(options, line, "user", null);
            String pwd = KSync3Utils.getPassword(line, url, user);

            File repoDir = new File(dir, ".ksync");
            repoDir.mkdirs();
            KSyncUtils.writeProps(url, user, repoDir);

            try {
                KSync3 kSync3 = new KSync3(dir, url, user, pwd, repoDir);
                kSync3.checkout(repoDir);
            } catch (IOException ex) {
                System.out.println("Could not checkout: " + ex.getMessage());
            }

        }, options);
    }

    private static void commit(Options options) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            k.commit(configDir);
        }, options);
        System.exit(0); // threads arent shutting down
    }

    private static void push(Options options) {
        log.info("push");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            log.info("do push {}", configDir);
            k.push(configDir);
        }, options);
        System.exit(0); // threads arent shutting down
    }

    private static void sync(Options options) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.start();
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, options);
        System.out.println("finished initial scan");

    }

    private static void pull(Options options) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.pull(configDir);
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, options);
        System.out.println("finished");
        System.exit(0); // threads arent shutting down
    }

    private final File localDir;
    private final EventManager eventManager;
    private final Host client;
    private final MemoryLocalTripletStore tripletStore;
    private final HttpBlobStore httpBlobStore;
    private final HttpHashStore httpHashStore;
    private final BlobStore localBlobStore;
    private final HashStore localHashStore;

    private final BlobStore wrappedBlobStore;
    private final HashStore wrappedHashStore;

    private final HashCalc hashCalc = HashCalc.getInstance();
    private final String branchPath;
    private final Counter transferQueueCounter = new Counter();
    
    private final LinkedBlockingQueue<Runnable> transferJobs = new LinkedBlockingQueue<>(100);
    private final CallerRunsPolicy rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    private final ExecutorService transferExecutor = new ThreadPoolExecutor(5, 20, 5, TimeUnit.SECONDS, transferJobs, rejectedExecutionHandler);
    

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir) throws MalformedURLException, IOException {
        this.localDir = localDir;
        eventManager = new EventManagerImpl();
        URL url = new URL(sRemoteAddress);
        client = new Host(url.getHost(), url.getPort(), user, pwd, null);
        client.setTimeout(30000);
        client.setUseDigestForPreemptiveAuth(false);
        branchPath = url.getFile();

        File repoDir = new File(localDir, ".ksync");
        this.localBlobStore = new FileSystem2BlobStore(new File(repoDir, "blobs"));
        this.localHashStore = new FileSystem2HashStore(new File(repoDir, "hashes"));

        HttpBloomFilterHashCache blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
        HttpBloomFilterHashCache chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");
        HttpBloomFilterHashCache fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");

        httpBlobStore = new HttpBlobStore(client, blobsHashCache);
        httpBlobStore.setBaseUrl("/_hashes/blobs/");

        httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
        httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
        httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

        wrappedBlobStore = new MultipleBlobStore(Arrays.asList(localBlobStore, httpBlobStore));
        wrappedHashStore = new MultipleHashStore(Arrays.asList(localHashStore, httpHashStore));

        log.info("Init {}", localDir.getAbsolutePath());

        tripletStore = new MemoryLocalTripletStore(localDir, eventManager, localBlobStore, localHashStore, (String rootHash) -> {
            try {
                log.info("File changed in {}, new repo hash {}", localDir, rootHash);
                push(rootHash, configDir);

            } catch (Exception ex) {
                log.error("Exception in file changed event handler", ex);
            }
        }, null, configDir);

    }

    private void start() throws MalformedURLException, IOException {
        log.info("Do initial scan");
        tripletStore.scan();
        log.info("Done initial scan, now begin monitoring..");
        tripletStore.start();
        log.info("Done monitor init");
    }


    private void push(String localRootHash, File configDir) throws IOException, InterruptedException {
        String remoteHash = getRemoteHash(branchPath);
        if (remoteHash == null) {
            log.info("Aborted");
            return;
        }
        if (remoteHash.equals(localRootHash)) {
            log.info("No change. Local repo is exactly the same as remote hash={}", localRootHash);
            return;
        }

        String lastRemoteHash = KSyncUtils.getLastRemoteHash(configDir);
        if (!remoteHash.equals(lastRemoteHash)) {
            log.info("Remote repository has changed, so pull and then re-scan");
            localRootHash = pull(configDir);
            log.info("New local hash={}", localRootHash);
        }

        // walk the VFS and push hashes and blobs to the remote store. Anything
        // already in the remote store will be ignored
        walkLocalVfs(localRootHash, httpBlobStore, httpHashStore, Path.root);

        // wait for threads to complete
        while (transferQueueCounter.count > 0) {
            Thread.sleep(300);
            log.info("Wait for transfers to complete..");
        }
        log.info("transfers complete");

        // Now set the hash on the repo, and check for any missing objects
        Map<String, String> params = new HashMap<>();
        params.put("newHash", localRootHash);
        params.put("validate", "true");
        try {
            log.info("PUSH Local: {} Remote: {}", localRootHash, remoteHash);
            String res = client.post(branchPath, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    KSyncUtils.saveRemoteHash(configDir, localRootHash);
                    log.info("Completed ok");
                    return;
                }
            }
            log.info("Push failed: Check for missing objects", res);
            // todo: check status
            Object dataOb = jsonRes.get("data");
            System.out.println("data: " + dataOb);
            JSONObject data = (JSONObject) dataOb;
            JSONArray missingChunksArr = (JSONArray) data.get("missingChunkFanouts");

            JSONArray missingBlobsArr = (JSONArray) data.get("missingBlobs");
            KSyncUtils.processHashes(missingBlobsArr, (String hash) -> {
                log.info("Upload missing blob {}", hash);
                byte[] arr = localBlobStore.getBlob(hash);
                httpBlobStore.setBlob(hash, arr);
            });

            KSyncUtils.processHashes(missingChunksArr, (String hash) -> {
                log.info("Upload missing chunk fanout {}", hash);
                Fanout fanout = localHashStore.getChunkFanout(hash);
                httpHashStore.setChunkFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
            });

            JSONArray missingFileFanoutsArr = (JSONArray) data.get("missingFileFanouts");
            KSyncUtils.processHashes(missingFileFanoutsArr, (String hash) -> {
                log.info("Upload missing file fanout {}", hash);
                Fanout fanout = localHashStore.getFileFanout(hash);
                httpHashStore.setFileFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
            });

            push(localRootHash, configDir);

            KSyncUtils.saveRemoteHash(configDir, localRootHash);

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            log.error("Exception setting hash", ex);
        }
    }

    private void walkLocalVfs(String dirHash, BlobStore httpBlobStore, HashStore httpHashStore, Path p) throws IOException, InterruptedException {
        //log.info("walk local vfs: {}", p);
        byte[] dirListBlob = localBlobStore.getBlob(dirHash);
        if (!httpBlobStore.hasBlob(dirHash)) {
            log.info("Push directory list for {}", p);
            //httpBlobStore.setBlob(dirHash, dirListBlob);
            transferQueueCounter.up();
            transferExecutor.submit(() -> {
                long tm = System.currentTimeMillis();
                System.out.println("upload " + dirHash);
                httpBlobStore.setBlob(dirHash, dirListBlob);
                transferQueueCounter.down();
                System.out.println("done upload " + dirHash);
                tm = System.currentTimeMillis() - tm;
                log.info("Uploaded blob in {} ms", tm);
            });
        }

        List<ITriplet> triplets = hashCalc.parseTriplets(new ByteArrayInputStream(dirListBlob));
        for (ITriplet triplet : triplets) {
            if (triplet.getType().equals("d")) {
                walkLocalVfs(triplet.getHash(), httpBlobStore, httpHashStore, p.child(triplet.getName()));
            } else {
                //log.info("Upload file: {}", triplet.getName());
                combineToRemote(p.child(triplet.getName()), triplet.getHash());
            }
        }
    }

    public void combineToRemote(Path filePath, String fileHash) throws InterruptedException {
        combine(filePath.toString(), fileHash, this.httpHashStore, this.httpBlobStore, localHashStore, localBlobStore);
    }

    public void combineToLocal(Path filePath, String fileHash) throws InterruptedException {
        combine(filePath.toString(), fileHash, localHashStore, localBlobStore, this.wrappedHashStore, this.wrappedBlobStore);
    }

    private void combine(String filePath, String fileHash, HashStore destHashStore, BlobStore destBlobStore, HashStore sourceHashStore, BlobStore sourceBlobStore) throws InterruptedException {
        if (destHashStore.hasFile(fileHash)) {
            return;
        }
        log.info("Copy file {}", filePath);
        Fanout fileFanout = sourceHashStore.getFileFanout(fileHash);

        final Counter c = new Counter();
        for (String fanoutHash : fileFanout.getHashes()) {
            Fanout fanout = sourceHashStore.getChunkFanout(fanoutHash);
            List<String> hashes = fanout.getHashes();
            for (String hash : hashes) {
                if (!destBlobStore.hasBlob(hash)) {
                    byte[] arr = sourceBlobStore.getBlob(hash);
                    c.up();
                    transferQueueCounter.up();
                    transferExecutor.submit(() -> {
                        log.info("Copy file chunk hash={} file={} chunk size={} to blobstore {}", hash, filePath, arr.length, destBlobStore);
                        destBlobStore.setBlob(hash, arr);
                        c.down();
                        transferQueueCounter.down();
                        log.info("Finished Copy file chunk hash={}", hash);
                    });
                    log.info("queue size {}", transferJobs.size());
                }
            }

            if (!destHashStore.hasChunk(fanoutHash)) {
                c.up();
                transferQueueCounter.up();
                transferExecutor.submit(() -> {
                    log.info("Upload chunk hash={} num hashes={} ", filePath, fanout.getHashes().size());
                    destHashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
                    c.down();
                    transferQueueCounter.down();
                    log.info("Finidh Upload chunk hash={} ", filePath);
                });

            }
        }

        if (!destHashStore.hasFile(fileHash)) {
            // wait for jobs to complete, we dont want to set the file hash until everything inside the file is uploaded
            log.info("set file hash1 queue size={} counter={}", transferJobs.size(), c.count);
            while (c.count > 0) {
                log.info("..waiting for transfers to complete. remaining={}", c.count);
                Thread.sleep(1000);
            }
            log.info("set file hash2");
            transferQueueCounter.up();
            transferExecutor.submit(() -> {
                log.info("Upload file hash={} hash={} ", filePath, fileHash);
                destHashStore.setFileFanout(fileHash, fileFanout.getHashes(), fileFanout.getActualContentLength());
                log.info("Done {}", fileHash);
                transferQueueCounter.down();
            });

        }
    }

    private void checkout(File configDir) {
        String hash = getRemoteHash(branchPath);
        try {
            fetch(Path.root, hash); // fetch into local blobstore
        } catch (InterruptedException ex) {
            log.error("interripted", ex);
            return;
        }
        pull(hash, this.localDir); // pull from local blobstore into local vfs
        KSyncUtils.saveRemoteHash(configDir, hash);
    }

    /**
     * Returns the new local hash
     *
     * @param configDir
     * @return
     * @throws IOException
     */
    public String pull(File configDir) throws IOException {
//        String localHash = commit(configDir);
        String lastRemoteHash = KSyncUtils.getLastRemoteHash(configDir);
        String remoteHash = getRemoteHash(branchPath);
        try {
            fetch(Path.root, remoteHash); // fetch into local blobstore
        } catch (InterruptedException ex) {
            log.error("interripted", ex);
            return null;
        }

        DeltaGenerator dg = new DeltaGenerator(wrappedHashStore, wrappedBlobStore, new FileUpdatingMergingDeltaListener(localDir, httpHashStore, httpBlobStore));
        dg.generateDeltas(lastRemoteHash, remoteHash); // calc changes and apply them to the working directory

        log.info("Finished pull, save hash " + remoteHash);
        KSyncUtils.saveRemoteHash(configDir, remoteHash);
        String newLocalHash = commit(configDir);
        return newLocalHash;
    }

    private String getRemoteHash(String path) {
        try {
            byte[] resp = client.get(path + "/?type=hash");
            String s = new String(resp);
            return s;
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException | NotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     *
     * @param hash
     */
    private void fetch(Path filePath, String hash) throws InterruptedException {
        _fetch(filePath, hash);
        log.info("fetch finished");
    }

    private void _fetch(Path filePath, String hash) throws InterruptedException {
        log.info("fetch: {}", filePath);
        List<ITriplet> triplets = getTriplets(hash, wrappedBlobStore);
        for (ITriplet t : triplets) {
            if (t.getType().equals("d")) {
                _fetch(filePath.child(t.getName()), t.getHash());
            } else {
                combineToLocal(filePath.child(t.getName()), t.getHash());
            }
        }
    }

    private List<ITriplet> getTriplets(String hash, BlobStore blobStore) {
        byte[] dir = blobStore.getBlob(hash);
        if (dir == null) {
            throw new RuntimeException("Could not find blob:" + hash);
        }
        localBlobStore.setBlob(hash, dir);
        try {
            return hashCalc.parseTriplets(new ByteArrayInputStream(dir));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void pull(String hash, File dir) {
        log.info("pull: " + dir.getAbsolutePath());
        List<ITriplet> triplets = getTriplets(hash, localBlobStore);
        for (ITriplet t : triplets) {
            if (t.getType().equals("d")) {
                File dir2 = new File(dir, t.getName());
                dir2.mkdirs();
                pull(t.getHash(), dir2);
            } else {
                Combiner c = new Combiner();
                File dest = new File(dir, t.getName());
                Fanout fileFanout = localHashStore.getFileFanout(t.getHash());
                try (FileOutputStream fout = new FileOutputStream(dest)) {
                    log.info("write local file: {}", dest.getAbsolutePath());
                    c.combine(fileFanout.getHashes(), localHashStore, localBlobStore, fout);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public String getBranchPath() {
        return branchPath;
    }

    private void push(File configDir) {
        String hash = commit(configDir);
        try {
            push(hash, configDir);
        } catch (IOException ex) {
            log.error("Ex", ex);
        } catch (InterruptedException ex) {
            log.info("Interrupted", ex);
        }
    }

    private String commit(File configDir) {
        String hash = tripletStore.scan();
        return hash;
    }

    private class Counter {

        private int count;

        synchronized void up() {
            count++;
        }

        synchronized void down() {
            count--;
        }
    }

}
