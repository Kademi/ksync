package co.kademi.sync;

import co.kademi.deploy.AppDeployer;
import io.milton.common.Path;
import io.milton.event.EventManager;
import io.milton.event.EventManagerImpl;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.http.values.Pair;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import io.milton.httpclient.HttpResult;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.triplets.DeltaGenerator;
import io.milton.sync.triplets.FileUpdatingMergingDeltaListener;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScheme;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
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
        commands.add(new LoginCommand());
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
        options.addOption("ignore", true, "Comma seperated list of file/folder names to ignore on checkout");

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
            showUsage(options);
            return;
        }

        cmd.execute(options, line);

        System.exit(0); // threads arent shutting down

    }

    private void showErrors() {
        if (!errors.isEmpty()) {
            System.out.println("----- ERRORS -------");
            for (String s : errors) {
                System.out.println(s);
            }
            System.out.println("----------------------");
        }
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
            commit(options, line);
        }

    }

    public static class PullCommand implements Command {

        @Override
        public String getName() {
            return "pull";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            pull(options, line);
        }

    }

    public static class PushCommand implements Command {

        @Override
        public String getName() {
            return "push";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            push(options, line);
        }

    }

    public static class SyncCommand implements Command {

        @Override
        public String getName() {
            return "sync";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            sync(options, line);
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
            } catch (Exception ex) {
                log.error("Invalie URL", ex);
            }
        }
    }

    public static class LoginCommand implements Command {

        @Override
        public String getName() {
            return "login";
        }

        @Override
        public void execute(Options options, CommandLine line) {
            login(options, line);
        }
    }

    public static void showUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ksync3", options);
    }

    private static void login(Options options, CommandLine line) {
        KSyncUtils.withDir((File dir) -> {
            String url = KSync3Utils.getInput(options, line, "url", null);
            String user = KSync3Utils.getInput(options, line, "user", null);
            String pwd = KSync3Utils.getPassword(line, url, user);

            File repoDir = new File(dir, ".ksync");
            try {
                KSync3 kSync3 = new KSync3(dir, url, user, pwd, repoDir, false, null, null);
                kSync3.login(null);
            } catch (Exception e) {
                System.out.println("Exception occured");
                e.printStackTrace();
            }

        }, options);

        System.exit(0);
    }

    private static void checkout(Options options, CommandLine line) {
        KSyncUtils.withKsync((KSync3 kSync3) -> {
            kSync3.checkout(kSync3.repoDir);
            kSync3.showErrors();
        }, options, line, true);

//        KSyncUtils.withDir((File dir) -> {
//            String url = KSync3Utils.getInput(options, line, "url", null);
//            String user = KSync3Utils.getInput(options, line, "user", null);
//            String pwd = KSync3Utils.getPassword(line, url, user);
//
//
//            File repoDir = new File(dir, ".ksync");
//            repoDir.mkdirs();
//            KSyncUtils.writeProps(url, user, repoDir);
//            Map cookies = KSyncUtils.getCookies(repoDir);
//            try {
//                KSync3 kSync3 = new KSync3(dir, url, user, pwd, repoDir, false, ignores, cookies);
//                kSync3.checkout(repoDir, ignores);
//                kSync3.showErrors();
//            } catch (IOException ex) {
//                System.out.println("Could not checkout: " + ex.getMessage());
//
//            }
//
//        }, options);
    }

    private static void commit(Options options, CommandLine line) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            k.commit();
            k.showErrors();
        }, line, options, false);
        System.exit(0); // threads arent shutting down
    }

    private static void push(Options options, CommandLine line) {
        log.info("push");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            log.info("do push {}", configDir);
            k.push(configDir);
            k.showErrors();
        }, line, options, false);
        System.exit(0); // threads arent shutting down
    }

    private static void sync(Options options, CommandLine line) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.start();
                k.showErrors();
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, line, options, true);
        System.out.println("finished initial scan");

    }

    private static void pull(Options options, CommandLine line) {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.pull(configDir);
                k.showErrors();
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, line, options, false);
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
    private final ExecutorService transferExecutor = new ThreadPoolExecutor(5, 10, 5, TimeUnit.SECONDS, transferJobs, rejectedExecutionHandler);
    private final File repoDir;
    private final File configDir;
    private final List<String> ignores;

    private final List<String> errors = new ArrayList<>();

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, List<String> ignores, Map<String, String> cookies) throws MalformedURLException, IOException {
        this.localDir = localDir;
        this.configDir = configDir;
        this.ignores = ignores;
        eventManager = new EventManagerImpl();
        URL url = new URL(sRemoteAddress);
        client = new Host(url.getHost(), url.getPort(), user, pwd, null);
        if (cookies != null && cookies.isEmpty()) {
            client.setUsePreemptiveAuth(true);
        } else {
            client.setUsePreemptiveAuth(false); // do not send Basic auth ,we want to use cookie authentication
        }
        boolean secure = url.getProtocol().equals("https");
        client.setSecure(secure);
        client.setTimeout(30000);
        client.setUseDigestForPreemptiveAuth(false);
        branchPath = url.getFile();
        if (cookies != null) {
            client.getCookies().putAll(cookies);
        }

        repoDir = new File(localDir, ".ksync");
        this.localBlobStore = new FileSystem2BlobStore(new File(repoDir, "blobs"));
        this.localHashStore = new FileSystem2HashStore(new File(repoDir, "hashes"));

        HttpBloomFilterHashCache blobsHashCache = null;
        try {
            blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
        } catch (Exception e) {
            log.warn("Unable to load blobs bloom filter, so things will be a bit slow: " + e);
            //log.error("exception", e);
        }

        HttpBloomFilterHashCache chunckFanoutHashCache = null;
        try {
            chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");
        } catch (Exception e) {
            log.warn("Unable to load chunks bloom filter, so things will be a bit slow " + e);
            //log.error("exception", e);
        }

        HttpBloomFilterHashCache fileFanoutHashCache = null;
        try {
            fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");
        } catch (Exception e) {
            log.warn("Unable to load files bloom filter, so things will be a bit slow");
            //log.error("exception", e);
        }

        httpBlobStore = new HttpBlobStore(client, blobsHashCache);
        httpBlobStore.setBaseUrl("/_hashes/blobs/");

        httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
        httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
        httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

        wrappedBlobStore = new MultipleBlobStore(Arrays.asList(localBlobStore, httpBlobStore));
        wrappedHashStore = new MultipleHashStore(Arrays.asList(localHashStore, httpHashStore));

        log.info("Init {}", localDir.getAbsolutePath());

        tripletStore = new MemoryLocalTripletStore(localDir, eventManager, localBlobStore, localHashStore, (String rootHash) -> {
            if (background) {
                try {
                    log.info("File changed in {}, new repo hash {}", localDir, rootHash);
                    push(rootHash, configDir);

                } catch (Exception ex) {
                    log.error("Exception in file changed event handler", ex);
                }
            }
        }, null, configDir, null, ignores);

    }

    private void start() throws MalformedURLException, IOException {
        log.info("Do initial scan");
        tripletStore.scan();
        log.info("Done initial scan, now begin monitoring..");
        tripletStore.start();
        log.info("Done monitor init");
    }

    private void login(String secondFactor) {
        log.info("login");

        HttpClient hc = this.client.getClient();
        HttpPost m = new HttpPost(this.client.baseHref());

        List<NameValuePair> formparams = new ArrayList<>();
        if (secondFactor != null) {
            formparams.add(new BasicNameValuePair("_login2FA", secondFactor));
        }
        UrlEncodedFormEntity entity;
        try {
            entity = new UrlEncodedFormEntity(formparams);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        m.setEntity(entity);
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            HttpResult result = executeHttpWithResult(hc, m, bout, newContext());
            int res = result.getStatusCode();
            switch (res) {
                case 401:
                    log.info("Authentication failed. Is 2FA required?");
                    String s = KSync3Utils.getInput("2FA code");
                    if (StringUtils.isNotBlank(s)) {
                        login(s);
                    } else {
                        log.info("Login aborted");
                    }
                    break;
                case 400:
                    log.info("Authentication failed. Is your userid correct?");
                    break;
                case 200:
                    log.info("login: completed {}", res);
                    // save auth token cookie to props file
                    boolean foundCookie = false;
                    List<String> cookies = result.getHeaderValues("Set-Cookie");
                    for (String setCookie : cookies) {
                        log.info("cookies: {}", setCookie);
                        // miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/; Expires=Wed, 04-Sep-2019 23:59:47 GMT
                        // miltonUserUrlHash="735e226b-2a44-4ed1-b058-1d3385ca861d:evwTfxqg682WLKGbpGyaSz7oWDc"; Path=/; Expires=Sat, 24-Aug-2019 02:29:54 GMT; HttpOnly
                        String[] arr = setCookie.split("\"");
                        String cookieName = arr[0];
                        if (cookieName.startsWith("miltonUserUrlHash")) {
                            String userUrlHash = arr[1];
                            String userUrl = "/users/" + this.client.user + "/";
                            KSyncUtils.writeLoginProps(userUrl, userUrlHash, this.repoDir);
                            foundCookie = true;
                            break;
                        }
                    }
                    if(!foundCookie){
                        log.warn("Login seemed to succeed, but didnt find an authorisation cookie");
                    }

                    break;

                default:
                    log.warn("login: unhandled result code: {}", res);
                    break;
            }
        } catch (IOException ex) {
            log.error("login: exception occured", ex);
        }
    }

    public static HttpResult executeHttpWithResult(HttpClient client, HttpUriRequest m, OutputStream out, HttpContext context) throws IOException {
        HttpResponse resp = client.execute(m, context);
        HttpEntity entity = resp.getEntity();
        if (entity != null) {
            InputStream in = null;
            try {
                in = entity.getContent();
                if (out != null) {
                    IOUtils.copy(in, out);
                }
            } finally {
                IOUtils.closeQuietly(in);
            }
        }

        Header[] respHeaders = resp.getAllHeaders();
        List<Pair<String, String>> allHeaders = new ArrayList<>();
        for (Header h : respHeaders) {
            allHeaders.add(new Pair(h.getName(), h.getValue())); // TODO: should concatenate multi-valued headers
        }
        HttpResult result = new HttpResult(resp.getStatusLine().getStatusCode(), allHeaders);
        return result;
    }

    protected HttpContext newContext() {
        HttpContext context = new BasicHttpContext();
        AuthScheme authScheme = new BasicScheme();
        context.setAttribute("preemptive-auth", authScheme);
        return context;
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
            log.info("Remote repository has changed, please pull. Current remote={} last remote={}", remoteHash, lastRemoteHash);
            return;
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
                //System.out.println("upload " + dirHash);
                httpBlobStore.setBlob(dirHash, dirListBlob);
                transferQueueCounter.down();
                //System.out.println("done upload " + dirHash);
                tm = System.currentTimeMillis() - tm;
                log.info("Transferred blob in {} ms", tm);
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
        Fanout ff = null;
        try {
            ff = sourceHashStore.getFileFanout(fileHash);

            Fanout fileFanout = ff;

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
                        log.info("Transfer chunk hash={} num hashes={} ", filePath, fanout.getHashes().size());
                        destHashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
                        c.down();
                        transferQueueCounter.down();
                        log.info("Finish transfer chunk hash={} ", filePath);
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
        } catch (Exception e) {
            String errMsg = "Could not retrieve file " + filePath + " because " + e.getMessage();
            errors.add(errMsg);
            log.error(errMsg, e);
            return;
        }
    }

    private void checkout(File configDir) {
        log.info("checkout {}", branchPath);
        String hash = getRemoteHash(branchPath);
        try {
            fetch(Path.root, hash, ignores); // fetch into local blobstore
        } catch (InterruptedException ex) {
            log.error("interripted", ex);
            return;
        }
        pull(hash, this.localDir, ignores); // pull from local blobstore into local vfs
        KSyncUtils.saveRemoteHash(configDir, hash);
        log.info("finished checkout");
    }

    /**
     * Returns the new local hash
     *
     * @param configDir
     * @return
     * @throws IOException
     */
    public String pull(File configDir) throws IOException {
        String localHash = commit();
        String lastRemoteHash = KSyncUtils.getLastRemoteHash(configDir);
        String remoteHash = getRemoteHash(branchPath);
        if (lastRemoteHash != null && lastRemoteHash.equals(remoteHash)) {
            log.info("No change on server since last pull");
            return null;
        }
        try {
            fetch(Path.root, remoteHash, null); // fetch into local blobstore
        } catch (InterruptedException ex) {
            log.error("interripted", ex);
            return null;
        }

        DeltaGenerator dg = new DeltaGenerator(wrappedHashStore, wrappedBlobStore, new FileUpdatingMergingDeltaListener(localDir, httpHashStore, httpBlobStore));
        dg.generateDeltas(lastRemoteHash, remoteHash, localHash); // calc changes and apply them to the working directory

        log.info("Finished pull, save hash " + remoteHash);
        KSyncUtils.saveRemoteHash(configDir, remoteHash);
        String newLocalHash = commit();
        return newLocalHash;
    }

    private String getRemoteHash(String path) {
        try {
            byte[] resp = client.get(path + "/?type=hash");
            if (resp == null) {
                return null;
            }
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
    private void fetch(Path filePath, String hash, List<String> ignores) throws InterruptedException {
        _fetch(filePath, hash, ignores);
        log.info("fetch finished");
    }

    private void _fetch(Path filePath, String hash, List<String> ignores) throws InterruptedException {
        log.info("fetch: {}", filePath);
        List<ITriplet> triplets;
        try {
            triplets = getTriplets(hash, wrappedBlobStore);
        } catch (Exception e) {
            String errMsg = "Could not fetch directory " + filePath + " because " + e.getMessage();
            log.error(errMsg, e);
            return;
        }
        if (triplets != null) {
            for (ITriplet t : triplets) {
                if (!KSync3Utils.ignored(t.getName(), ignores)) {
                    if (t.getType().equals("d")) {
                        _fetch(filePath.child(t.getName()), t.getHash(), ignores);
                    } else {
                        combineToLocal(filePath.child(t.getName()), t.getHash());
                    }
                }
            }
        }
    }

    private List<ITriplet> getTriplets(String hash, BlobStore blobStore) {
        if (hash == null || hash.equals("null")) {
            return null;
        }
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

    private void pull(String hash, File dir, List<String> ignores) {
        log.info("pull: " + dir.getAbsolutePath());
        if (hash == null) {
            log.info("pull: hash is null, so nothing");
            return;
        }
        List<ITriplet> triplets;
        try {
            triplets = getTriplets(hash, localBlobStore);
        } catch (Exception e) {
            String errMsg = "Could not pull directory for " + dir.getAbsolutePath() + " with hash " + hash + " because " + e.getMessage();
            errors.add(errMsg);
            log.warn(errMsg, e);
            return;
        }
        if (triplets != null) {
            for (ITriplet t : triplets) {
                if (!KSync3Utils.ignored(t.getName(), ignores)) {
                    if (t.getType().equals("d")) {
                        File dir2 = new File(dir, t.getName());
                        dir2.mkdirs();
                        pull(t.getHash(), dir2, ignores);
                    } else {
                        Combiner c = new Combiner();
                        File dest = new File(dir, t.getName());
                        Fanout fileFanout = localHashStore.getFileFanout(t.getHash());
                        if (fileFanout != null) {
                            try (FileOutputStream fout = new FileOutputStream(dest)) {
                                log.info("write local file: {}", dest.getAbsolutePath());
                                c.combine(fileFanout.getHashes(), localHashStore, localBlobStore, fout);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        } else {
                            String errMsg = "Could not get file fanout for hash " + t.getHash() + " in directory " + dir.getAbsolutePath();
                            errors.add(errMsg);
                            log.warn(errMsg);
                        }
                    }
                }
            }
        }
    }

    public String getBranchPath() {
        return branchPath;
    }

    public List<String> getIgnores() {
        return ignores;
    }

    public File getConfigDir() {
        return configDir;
    }

    private void push(File configDir) {
        String hash = commit();
        try {
            push(hash, configDir);
        } catch (IOException ex) {
            log.error("Ex", ex);
        } catch (InterruptedException ex) {
            log.info("Interrupted", ex);
        }
    }

    private String commit() {
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
