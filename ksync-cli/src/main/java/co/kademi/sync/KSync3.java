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
import io.milton.sync.ConflictResolvers;
import co.kademi.sync.oauth.NotLoggedInException;
import co.kademi.sync.oauth.OAuth2Client;
import io.milton.httpclient.HttpException;
import io.milton.httpclient.HttpResult;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.triplets.BerkeleyDbFileHashCache;
import io.milton.sync.triplets.DeltaGenerator;
import io.milton.sync.triplets.FileSystemWatchingService;
import io.milton.sync.triplets.FileUpdatingMergingDeltaListener;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import io.milton.sync.triplets.SyncHashCache;
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
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
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
import org.hashsplit4j.api.HashCache;
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

    /** How conflicts are asked about, from -conflictmode. */
    private static ConflictResolvers.Mode conflictMode = ConflictResolvers.Mode.AUTO;

    private static final List<Command> commands = new ArrayList<>();

    static {
        commands.add(new UsageCommand());
        commands.add(new CheckoutCommand());
        commands.add(new PushCommand());
        commands.add(new PullCommand());
        commands.add(new SyncCommand());
        commands.add(new PublishCommand());
        commands.add(new LoginCommand());
        commands.add(new IgnoreCommand());
    }

    public static void main(String[] arg) {

        // Check how many arguments were passed in
        if (arg == null || arg.length == 0) {
            log.error("No arguments given. Run ksync3 -command usage to see the options");
            System.exit(0);
        }

        if (KSyncUri.isUri(arg)) {
            log.debug("Found a ksync uri, parsing its arguments");
            arg = KSyncUri.parseArguments(arg);
        }

        KSync3.handleKSync(arg);

    }

    private static void handleKSync(String[] arg) {
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
        options.addOption("auth", true, "An encrypted token from the server which provides authentication");
        options.addOption("appname", true, "app name for creating folder in app directory");
        options.addOption("appdir", true, "defines whether ksync was executed from an URI schema or from terminal");
        options.addOption("conflictmode", true, "How to ask about file conflicts: gui (a dialog, the default), console (a terminal prompt, for CI or an agent), or auto");
        options.addOption("debug", false, "Verbose output: show debug logging, with the level and source class on each line");
        options.addOption("logformat", true, "Shape of each log line: plain (the message alone, the default), ts (an ISO-8601 UTC timestamp and level first) or kv (ts=.. level=.. msg=\"..\", for a log reader)");
        options.addOption("oauth", false, "Use OAuth2 for the login command, instead of a username and password. Opens a browser to authorize");
        options.addOption("logout", false, "Discard the stored OAuth2 tokens (for the login command)");
        options.addOption("pattern", true, "File/folder name or glob to add to the global ignore file, eg *.log or node_modules. Comma seperated for several (for the ignore command). Lists the file when omitted");
        CommandLineParser parser = new DefaultParser();
        CommandLine line;
        try {
            // parse the command line arguments
            line = parser.parse(options, arg);
        } catch (Exception exp) {
            // oops, something went wrong
            log.error("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
            return;
        }

        // Both of these reject an unknown value by name. That is a typo in the command line, not a
        // fault worth a stack trace, so report it the same way a parse failure is reported.
        try {
            configureLogging(KSync3Utils.getBooleanInput(line, "debug"), line.getOptionValue("logformat"));
            conflictMode = ConflictResolvers.parseMode(line.getOptionValue("conflictmode"));
        } catch (IllegalArgumentException ex) {
            log.error(ex.getMessage());
            System.exit(1);
            return;
        }

        Command cmd = KSync3Utils.findCommand(line, commands);

        if (cmd == null) {
            showUsage(options);
            return;
        }

        try {
            cmd.execute(options, line);
        } catch (Exception ex) {
            NotLoggedInException notLoggedIn = NotLoggedInException.find(ex);
            if (notLoggedIn != null) {
                // nothing in the stack trace helps the person reading it; they just need to log in
                log.error(notLoggedIn.getMessage());
                System.exit(1);
            }
            log.error("Exception running command {} - {}", cmd.getName(), ex.getMessage(), ex);
            System.exit(1);
        }

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

    private final BlockingQueue<Runnable> fileDownloadQueue = new ArrayBlockingQueue<>(100000); // used for checkout command
    private final ExecutorService fileTransferExecutor = new ThreadPoolExecutor(20, 20, 60, TimeUnit.SECONDS, fileDownloadQueue);
    private final List<Future> fileDownloadFutures = new ArrayList<>();

    public interface Command {

        String getName();

        void execute(Options options, CommandLine line) throws Exception;
    }

    public static class CheckoutCommand implements Command {

        @Override
        public String getName() {
            return "checkout";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
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
        public void execute(Options options, CommandLine line) throws Exception {
            commit(options, line);
        }

    }

    public static class PullCommand implements Command {

        @Override
        public String getName() {
            return "pull";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            pull(options, line);
        }

    }

    public static class PushCommand implements Command {

        @Override
        public String getName() {
            return "push";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            push(options, line);
        }

    }

    public static class SyncCommand implements Command {

        @Override
        public String getName() {
            return "sync";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            sync(options, line);
            boolean done = false;
            while (!done) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException interruptedException) {
                    done = true;
                }
            }
        }

    }

    public static class PublishCommand implements Command {

        @Override
        public String getName() {
            return "publish";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            AppDeployer.publish(options, line);
        }
    }

    public static class LoginCommand implements Command {

        @Override
        public String getName() {
            return "login";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            login(options, line);
        }
    }

    public static class IgnoreCommand implements Command {

        @Override
        public String getName() {
            return "ignore";
        }

        @Override
        public void execute(Options options, CommandLine line) throws Exception {
            ignore(options, line);
        }
    }

    /**
     * ISO-8601 in UTC. A log being read by another program should not shift when the machine
     * writing it is in a different zone, or when it crosses a daylight saving boundary mid-sync.
     */
    private static final String TIMESTAMP = "%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}{UTC}";

    /**
     * The log4j2 conversion pattern for a -logformat value, or null to leave the configured
     * default (the message alone) in place.
     *
     * kv quotes the message and escapes what is inside it, because a sync message can carry a file
     * name with a quote in it and an unescaped one would end the field early.
     */
    private static String logPattern(String logFormat, boolean debug) {
        if (StringUtils.isBlank(logFormat)) {
            // -debug on its own still wants the level and source class, as it always has.
            return debug ? "%-5p %c - %m%n" : null;
        }
        String f = logFormat.trim().toLowerCase(Locale.ROOT);
        // The source class earns its place once someone is diagnosing, whichever shape they chose.
        String source = debug ? " %c" : "";
        switch (f) {
            case "plain":
                return debug ? "%-5p" + source + " - %m%n" : "%m%n";
            case "ts":
                return TIMESTAMP + " %-5p" + source + " %m%n";
            case "kv":
                return "ts=" + TIMESTAMP + " level=%p"
                        + (debug ? " source=%c" : "")
                        + " msg=\"%enc{%m}{JSON}\"%n";
            default:
                throw new IllegalArgumentException("Unknown log format '" + logFormat
                        + "'. Use one of: plain, ts, kv");
        }
    }

    /**
     * Normal runs print the message alone; the level and class name only help when something is
     * being diagnosed, and they bury the lines a user actually wants. -debug brings both back and
     * turns the level up, and -logformat asks for a shape a program can read.
     */
    private static void configureLogging(boolean debug, String logFormat) {
        String pattern = logPattern(logFormat, debug);
        if (pattern != null) {
            // log4j2 resolves ${sys:ksync.logPattern} when it builds the layout, which has already
            // happened by now because our static loggers initialised it. Setting the property and
            // reconfiguring is what makes the new pattern take effect.
            System.setProperty("ksync.logPattern", pattern);
            org.apache.logging.log4j.core.config.Configurator.reconfigure();
        }
        if (debug) {
            // Only our own code, not every library: turning httpclient up to debug buries everything
            // in wire logging nobody asked for.
            org.apache.logging.log4j.core.config.Configurator.setLevel("co.kademi", org.apache.logging.log4j.Level.DEBUG);
            org.apache.logging.log4j.core.config.Configurator.setLevel("io.milton.sync", org.apache.logging.log4j.Level.DEBUG);
            log.debug("Debug logging enabled");
        }
    }

    public static void showUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ksync3", options);
    }

    private static void login(Options options, CommandLine line) throws Exception {
        log.info("Signing in..");
        KSyncUtils.withDir((File dir) -> {
            File repoDir = new File(dir, ".ksync");
            repoDir.mkdirs();
            Properties props = KSyncUtils.readProps(repoDir);
            String url = KSync3Utils.getInput(options, line, "url", props, true);

            if (KSync3Utils.getBooleanInput(line, "logout")) {
                KSyncUtils.newOAuth2Client(url).logout();
                log.info("Logged out, stored credentials for this site discarded");
                return;
            }

            if (KSync3Utils.getBooleanInput(line, "oauth")) {
                KSyncUtils.writeProps(url, null, repoDir);
                KSyncUtils.newOAuth2Client(url).login();
                return;
            }

            String user = KSync3Utils.getInput(options, line, "user", props, true);
            String pwd = KSync3Utils.getPassword(line, url, user);

            KSync3 kSync3 = new KSync3(dir, url, user, pwd, repoDir, false, null, null);
            kSync3.login(null);
        }, options, line);
    }

    /**
     * Adds patterns to the global ignore file, or shows what is in it.
     *
     * Unlike the other commands this one is not about any one checkout, so it does not need a
     * directory, a url or a login.
     */
    private static void ignore(Options options, CommandLine line) throws Exception {
        GlobalIgnores ignores = GlobalIgnores.defaultIgnores();
        List<String> toAdd = KSync3Utils.split(line.getOptionValue("pattern"));

        if (toAdd == null) {
            showIgnores(ignores);
            return;
        }

        for (String pattern : toAdd) {
            if (StringUtils.isBlank(pattern)) {
                continue;
            }
            if (ignores.add(pattern)) {
                log.info("Ignoring {}", pattern);
            } else {
                log.info("Already ignoring {}", pattern);
            }
        }
        log.info("Global ignore file is {}", ignores.getPath());
    }

    private static void showIgnores(GlobalIgnores ignores) {
        List<String> patterns = ignores.patterns();
        if (patterns.isEmpty()) {
            log.info("No global ignore patterns yet. Add one with: ksync3 -command ignore -pattern \"*.log\"");
            log.info("They would be kept in {}", ignores.getPath());
            return;
        }
        log.info("Global ignore patterns, from {}:", ignores.getPath());
        for (String pattern : patterns) {
            log.info("  {}", pattern);
        }
    }

    private static void checkout(Options options, CommandLine line) throws Exception {
        log.info("Checking out..");

        KSyncUtils.withKsync((KSync3 kSync3) -> {
            kSync3.checkout(kSync3.repoDir);
            kSync3.showErrors();
        }, options, line, true, false);

    }

    private static void commit(Options options, CommandLine line) throws Exception {
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            k.commit();
            k.showErrors();
        }, line, options, false);
        System.exit(0); // threads arent shutting down
    }

    private static void push(Options options, CommandLine line) throws Exception {
        log.info("Pushing local changes..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            log.debug("do push {}", configDir);
            k.push(configDir);
            k.showErrors();
        }, line, options, false);
        System.exit(0); // threads arent shutting down
    }

    private static void sync(Options options, CommandLine line) throws Exception {
        log.info("Syncing..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            k.start();
            k.showErrors();
        }, line, options, true);
        log.info("Finished the initial scan");

    }

    private static void pull(Options options, CommandLine line) throws Exception {
        log.info("Pulling changes from the server..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.pull(configDir);
                k.showErrors();
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, line, options, false);
        log.info("Done");
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

    private final LinkedBlockingQueue<Runnable> transferJobs = new LinkedBlockingQueue<>(1000);
    private final CallerRunsPolicy rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    private final ExecutorService transferExecutor = new ThreadPoolExecutor(5, 10, 5, TimeUnit.SECONDS, transferJobs, rejectedExecutionHandler);
    private final File repoDir;
    private final File configDir;
    private final List<String> ignores;
    private final SyncHashCache fileHashCache;
    private final FileSystemWatchingService fileSystemWatchingService;
    private final ScheduledExecutorService scheduledExecutorService;

    private final List<String> errors = new ArrayList<>();
    private final String remoteAddress;

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, List<String> ignores, Map<String, String> cookies) throws MalformedURLException, IOException {
        this(localDir, sRemoteAddress, user, pwd, configDir, background, ignores, cookies, null);
    }

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, List<String> ignores, Map<String, String> cookies, OAuth2Client oauth) throws MalformedURLException, IOException {
        this.localDir = localDir;
        this.configDir = configDir;
        this.ignores = ignores;
        this.remoteAddress = sRemoteAddress;
        eventManager = new EventManagerImpl();

        int timeout = 180000;
        URL url = new URL(sRemoteAddress);
        if (oauth != null) {
            // Bearer auth carries the identity, so no cookies and no Basic auth
            client = new BearerHost(url.getHost(), null, url.getPort(), user, oauth::accessToken, timeout);
            cookies = null;
        } else {
            client = new Host(url.getHost(), null, url.getPort(), user, pwd, null, timeout, new java.util.concurrent.ConcurrentHashMap<>(), null);
            if (cookies != null && cookies.isEmpty()) {
                client.setUsePreemptiveAuth(true);
            } else {
                client.setUsePreemptiveAuth(false); // do not send Basic auth ,we want to use cookie authentication
            }
        }
        boolean secure = url.getProtocol().equals("https");
        client.setSecure(secure);
        client.setTimeout(timeout);
        log.debug("Using timeout of " + timeout + "ms");
        client.setUseDigestForPreemptiveAuth(false);
        branchPath = url.getFile();
        if (cookies != null) {
            client.getCookies().putAll(cookies);
        }

        repoDir = new File(localDir, ".ksync");
        this.localBlobStore = new FileSystem2BlobStore(new File(repoDir, "blobs"));
        this.localHashStore = new FileSystem2HashStore(new File(repoDir, "hashes"));

        // The bloom filters tell us what the server already has, and each one costs the server a full walk of the
        // repository to produce, so they are built on first use rather than eagerly here. A checkout that gets a pack
        // never touches the http stores at all and so never asks for them, and neither do login or commit. The
        // per-object fallback still does, because HttpHashStore and HttpBlobStore consult the caches in hasFile,
        // hasChunk and hasBlob.
        HashCache blobsHashCache = new LazyHashCache("Blobs Bloom Filter",
                () -> new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom"));
        HashCache chunckFanoutHashCache = new LazyHashCache("Chunks Bloom Filter",
                () -> new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom"));
        HashCache fileFanoutHashCache = new LazyHashCache("Files Bloom Filter",
                () -> new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom"));

        httpBlobStore = new HttpBlobStore(client, blobsHashCache);
        httpBlobStore.setBaseUrl("/_hashes/blobs/");

        httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
        httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
        httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

        wrappedBlobStore = new MultipleBlobStore(Arrays.asList(localBlobStore, httpBlobStore));
        wrappedHashStore = new MultipleHashStore(Arrays.asList(localHashStore, httpHashStore));

        log.debug("Init {}", localDir.getAbsolutePath());

        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        final java.nio.file.Path path = FileSystems.getDefault().getPath(localDir.getAbsolutePath());
        WatchService watchService = null;
        try {
            watchService = path.getFileSystem().newWatchService();
        } catch (IOException ex) {
            log.error("Exception initialising watch service", ex);
        }
        if (watchService != null) {
            fileSystemWatchingService = new FileSystemWatchingService(watchService, scheduledExecutorService);
        } else {
            fileSystemWatchingService = null;
        }

        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File envDir = new File(tmpDir, "appDeployer-filecache-" + KSync3Utils.makeFileName(sRemoteAddress));
        fileHashCache = new BerkeleyDbFileHashCache(envDir);

        tripletStore = new MemoryLocalTripletStore(localDir, eventManager, localBlobStore, localHashStore, (String rootHash) -> {
            if (background) {
                try {
                    log.debug("File changed in {}, new repo hash {}", localDir, rootHash);
                    push(rootHash, configDir);

                } catch (Exception ex) {
                    log.error("Exception in file changed event handler", ex);
                }
            }
        }, null, fileSystemWatchingService, ignores, fileHashCache);
//        MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), blobStore, hashStore, (String rootHash) -> {
//            needsPush.set(true);
//        }, null, fileWatchService, null, fileHashCache);

    }

    private void start() throws MalformedURLException, IOException {
        log.info("Do initial scan");
        tripletStore.scan();
        log.info("Done initial scan, now begin monitoring..");
        tripletStore.start();
        log.debug("Done monitor init");
    }

    private void login(String secondFactor) {
        log.debug("login");

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
                    log.debug("login: completed {}", res);
                    // save auth token cookie to props file
                    boolean foundCookie = false;
                    List<String> cookies = new ArrayList<>();

                    Map<String, String> headers = result.getHeaders();
                    if (headers != null) {
                        String cookieString = headers.get("Set-Cookie");

                        String[] cookieStrings = StringUtils.split(cookieString, "\n");
                        cookies = Arrays.asList(cookieStrings);
                    }
                    //List<String> cookies = result.getHeaderValues("Set-Cookie");
                    for (String setCookie : cookies) {
                        log.debug("Parsing a Set-Cookie header from the login response");
                        // miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/; Expires=Wed, 04-Sep-2019 23:59:47 GMT
                        // miltonUserUrlHash="YYY-XXX-YYY-ZZZ-XXX:DDDD"; Path=/; Expires=Sat, 24-Aug-2019 02:29:54 GMT; HttpOnly
                        String[] arr = setCookie.split("\"");
                        String cookieName = arr[0];
                        if (cookieName.startsWith("miltonUserUrlHash")) {
                            String userUrlHash = arr[1];
                            String userUrl = "/users/" + this.client.user + "/";
                            KSyncUtils.writeLoginProps(userUrl, userUrlHash, this.remoteAddress);
                            foundCookie = true;
                            break;
                        }
                    }
                    if (!foundCookie) {
                        // Now try using the format where all cookies are in one line:
                        // miltonUserUrl=b64L3VzZXJzL2thZGVtaWJyYWQv; Path=/; Expires=Sat, 21-Mar-2020 02:05:55 GMT, miltonUserUrlHash="dd-dd-dd-dd-dd:ddd"; Path=/; Expires=Sat, 21-Mar-2020 02:05:55 GMT; HttpOnly
                        for (String setCookie : cookies) {
                            log.debug("Parsing a combined Set-Cookie header from the login response");
                            String key = "miltonUserUrlHash=\"";
                            int pos = setCookie.indexOf(key);
                            if (pos > 0) {
                                String hash = setCookie.substring(pos + key.length());
                                // deliberately not logged: this is the auth hash itself
                                hash = hash.substring(0, hash.indexOf("\""));
                                
                                String userUrlHash = hash;
                                String userUrl = "/users/" + this.client.user + "/";
                                KSyncUtils.writeLoginProps(userUrl, userUrlHash, this.remoteAddress);
                                foundCookie = true;
                                break;
                            }
                        }
                    }

                    if (!foundCookie) {
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

        Map<String, String> mapHeaders = new LinkedHashMap();

        Header[] respHeaders = resp.getAllHeaders();
        //List<Pair<String, String>> allHeaders = new ArrayList<>();
        for (Header h : respHeaders) {
            //allHeaders.add(new Pair(h.getName(), h.getValue())); // TODO: should concatenate multi-valued headers

            String headerValue = mapHeaders.get(h.getName());
            if (headerValue == null) {
                mapHeaders.put(h.getName(), h.getValue());
            } else {
                mapHeaders.put(h.getName(), headerValue + ", " + h.getValue());
            }
        }
        List<Pair<String, String>> mapHeaders2 = new ArrayList<>();
        for (Map.Entry<String, String> entry : mapHeaders.entrySet()) {
            mapHeaders2.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        HttpResult result = new HttpResult(resp.getStatusLine().getStatusCode(), mapHeaders2);
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
        log.debug("Wait for push transfers to complete..");
        while (transferQueueCounter.count > 0) {
            Thread.sleep(300);
        }
        log.info("Push complete");

        // Now set the hash on the repo, and check for any missing objects
        Map<String, String> params = new HashMap<>();
        params.put("newHash", localRootHash);
        params.put("validate", "true");
        try {
            log.debug("PUSH Local: {} Remote: {}", localRootHash, remoteHash);
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
            log.warn("Push failed: Check for missing objects", res);
            // todo: check status
            Object dataOb = jsonRes.get("data");
            log.debug("Push failure payload: {}", dataOb);
            JSONObject data = (JSONObject) dataOb;
            JSONArray missingChunksArr = (JSONArray) data.get("missingChunkFanouts");

            JSONArray missingBlobsArr = (JSONArray) data.get("missingBlobs");
            KSyncUtils.processHashes(missingBlobsArr, (String hash) -> {
                byte[] arr = localBlobStore.getBlob(hash);
                log.debug("Upload missing blob {} size={} to blobstore={}", hash, arr.length, httpBlobStore);
                try {
                    httpBlobStore.setForce(true);
                    httpBlobStore.setBlob(hash, arr);
                } finally {
                    httpBlobStore.setForce(false);
                }
            });

            KSyncUtils.processHashes(missingChunksArr, (String hash) -> {
                log.debug("Upload missing chunk fanout {}", hash);
                Fanout fanout = localHashStore.getChunkFanout(hash);
                try {
                    httpHashStore.setForce(true);
                    httpHashStore.setChunkFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
                } finally {
                    httpHashStore.setForce(false);
                }
            });

            JSONArray missingFileFanoutsArr = (JSONArray) data.get("missingFileFanouts");
            KSyncUtils.processHashes(missingFileFanoutsArr, (String hash) -> {
                log.debug("Upload missing file fanout {}", hash);
                Fanout fanout = localHashStore.getFileFanout(hash);
                try {
                    httpHashStore.setForce(true);
                    httpHashStore.setFileFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
                } finally {
                    httpHashStore.setForce(false);
                }
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
            log.debug("Push directory list for {}", p);
            //httpBlobStore.setBlob(dirHash, dirListBlob);
            transferQueueCounter.up();
            transferExecutor.submit(() -> {
                long tm = System.currentTimeMillis();
                //System.out.println("upload " + dirHash);
                httpBlobStore.setBlob(dirHash, dirListBlob);
                transferQueueCounter.down();
                //System.out.println("done upload " + dirHash);
                tm = System.currentTimeMillis() - tm;
                log.debug("Transferred blob in {} ms", tm);
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
        combine(filePath.toString(), fileHash, this.httpHashStore, this.httpBlobStore, localHashStore, localBlobStore, false);
    }

    public void combineToLocal(Path filePath, String fileHash) throws InterruptedException {
        combine(filePath.toString(), fileHash, localHashStore, localBlobStore, this.wrappedHashStore, this.wrappedBlobStore, true);
    }

    /**
     * Copies a file's fanouts and blobs from one pair of stores to another.
     *
     * @param synchronous when true the writes happen on this thread. Writing to the remote is done on the transfer
     * executor so uploads overlap, and the file fanout must not be set until they finish, which is what the wait below
     * is for. Writing to the local stores is just disk IO - the expensive part, fetching from the source, has already
     * happened on this thread - so queueing it buys nothing and the wait would cost a second per file.
     */
    private void combine(String filePath, String fileHash, HashStore destHashStore, BlobStore destBlobStore, HashStore sourceHashStore, BlobStore sourceBlobStore, boolean synchronous) throws InterruptedException {
        if (destHashStore.hasFile(fileHash)) {
            return;
        }
        //log.info("Copy file {}", filePath);
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
                        if (synchronous) {
                            destBlobStore.setBlob(hash, arr);
                        } else {
                            c.up();
                            transferQueueCounter.up();
                            transferExecutor.submit(() -> {
                                log.debug("transfer blob for file {} with size {} bytes", filePath, arr.length);
                                destBlobStore.setBlob(hash, arr);
                                c.down();
                                transferQueueCounter.down();
                            });
                        }
                    }
                }

                if (!destHashStore.hasChunk(fanoutHash)) {
                    if (synchronous) {
                        destHashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
                    } else {
                        c.up();
                        transferQueueCounter.up();
                        transferExecutor.submit(() -> {
                            log.debug("Transfer chunk for file {}", filePath);
                            destHashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
                            c.down();
                            transferQueueCounter.down();
                        });
                    }
                }
            }

            if (!destHashStore.hasFile(fileHash)) {
                if (synchronous) {
                    destHashStore.setFileFanout(fileHash, fileFanout.getHashes(), fileFanout.getActualContentLength());
                } else {
                    // wait for jobs to complete, we dont want to set the file hash until everything inside the file is uploaded
                    log.debug("Waiting for transfers to complete");
                    while (c.count > 0) {
                        Thread.sleep(1000);
                    }
                    transferQueueCounter.up();
                    transferExecutor.submit(() -> {
                        destHashStore.setFileFanout(fileHash, fileFanout.getHashes(), fileFanout.getActualContentLength());
                        transferQueueCounter.down();
                    });
                }
            }
        } catch (Exception e) {
            String errMsg = "Could not retrieve file " + filePath + " because " + e.getMessage();
            errors.add(errMsg);
            log.error(errMsg, e);
        }
    }

    private void checkout(File configDir) {
        log.info("checkout {}", branchPath);
        String hash = getRemoteHash(branchPath);

        // Checkout starts from nothing, so ask the server for the whole object graph in one request. Anything the
        // server cannot pack, or a server too old to know about packs, drops us back to walking it object by object.
        PackFetcher packFetcher = new PackFetcher(client, localBlobStore, localHashStore);
        if (packFetcher.fetch(hash)) {
            errors.addAll(packFetcher.getErrors());
        } else {
            try {
                fetch(Path.root, hash, ignores); // fetch into local blobstore
            } catch (InterruptedException ex) {
                log.error("interripted", ex);
                return;
            }
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

        DeltaGenerator dg = new DeltaGenerator(wrappedHashStore, wrappedBlobStore, new FileUpdatingMergingDeltaListener(localDir, httpHashStore, httpBlobStore,
                ConflictResolvers.create(conflictMode)));
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
        try {
            startFileDownloads();
            _fetch(filePath, hash, ignores);
            log.debug("Waiting for file downloads to finish.");
            while (!areDownloadsFinished()) {
                Thread.sleep(500);
            }
        } finally {
            stopFileDownloads();
        }
        log.debug("fetch finished");
    }

    private void _fetch(Path filePath, String hash, List<String> ignores) throws InterruptedException {
        log.debug("fetch: {}", filePath);
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
                        enqueueFileDownload(filePath.child(t.getName()), t.getHash());
                        //combineToLocal(filePath.child(t.getName()), t.getHash());
                    }
                }
            }
        }
    }

    private boolean areDownloadsFinished() {
        if (!fileDownloadQueue.isEmpty()) {
            return false;
        }
        for (Future f : fileDownloadFutures) {
            if (!f.isDone()) {
                return false;
            }
        }
        return true;
    }

    private void enqueueFileDownload(Path filePath, String hash) {
        //fileDownloadQueue.add(hash);
        Future<?> f = fileTransferExecutor.submit(() -> {
            try {
                combineToLocal(filePath, hash);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
        if (f != null) {
            fileDownloadFutures.add(f);
        }
    }

    private void startFileDownloads() {
        fileDownloadFutures.clear();
    }

    private void stopFileDownloads() {
        fileDownloadFutures.clear();
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
        log.debug("pull: " + dir.getAbsolutePath());
        if (hash == null) {
            log.debug("pull: hash is null, so nothing");
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
                                log.debug("write local file: {}", dest.getAbsolutePath());
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
            log.warn("Interrupted", ex);
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
