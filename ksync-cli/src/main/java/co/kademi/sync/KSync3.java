package co.kademi.sync;

import co.kademi.sync.status.SyncState;
import co.kademi.sync.status.SyncStatusReporter;
import co.kademi.sync.status.TrayStatusIcon;
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
import co.kademi.sync.commands.BaseCommand;
import co.kademi.sync.commands.CheckoutCommand;
import co.kademi.sync.commands.ConflictOptions;
import co.kademi.sync.commands.IgnoreCommand;
import co.kademi.sync.commands.InitCommand;
import co.kademi.sync.commands.LoginCommand;
import co.kademi.sync.commands.LogoutCommand;
import co.kademi.sync.commands.PullCommand;
import co.kademi.sync.commands.PushCommand;
import co.kademi.sync.commands.SyncCommand;
import co.kademi.sync.commands.VerifyCommand;
import co.kademi.sync.oauth.CredentialStore;
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
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    /**
     * How conflicts are asked about, from -conflictmode.
     */
    private static ConflictResolvers.Mode conflictMode = ConflictResolvers.Mode.AUTO;

    /**
     * Whether local is the authority, from -localwins.
     *
     * Off by default, which is the careful reading: the remote may hold someone
     * else's work, so a remote that has moved on stops a push and a conflict
     * gets a question. Where the local checkout is version managed that reading
     * is wrong - the remote differing is the normal state of things, and both
     * interruptions are just noise in the way.
     */
    private static boolean localWins;

    /**
     * Where to write the status JSON, from -statusfile. Null means the default
     * inside .ksync.
     *
     * Fully qualified because io.milton.common.Path is the Path in this file.
     */
    private static java.nio.file.Path statusFile;

    /**
     * Whether the status bar icon was turned off with -notray.
     */
    private static boolean trayDisabled;

    /**
     * The command being run, which decides whether status is worth publishing
     * at all.
     */
    private static String commandName;

    public static void main(String[] arg) {
        if (arg != null && arg.length > 0 && KSyncUri.isUri(arg)) {
            log.debug("Found a ksync uri, parsing its arguments");
            arg = KSyncUri.parseArguments(arg);
        }
        System.exit(Cli.run(arg == null ? new String[0] : arg));
    }

    /**
     * Applies the options every command shares, before the command itself runs.
     *
     * These live in statics because the objects that read them are built deep
     * inside a sync and have no route back to the command line.
     *
     * @param cmd
     */
    public static void configure(BaseCommand cmd) {
        commandName = cmd.spec.name();
        // Both of these reject an unknown value by name. That is a typo on the command line, not a
        // fault worth a stack trace, so report it the way picocli reports a bad option.
        try {
            configureLogging(cmd.global.debug, cmd.global.logformat);
            ConflictOptions conflict = cmd.conflict();
            if (conflict != null) {
                localWins = conflict.localwins;
                conflictMode = conflict.conflictmode;
                statusFile = StringUtils.isBlank(conflict.statusfile) ? null : Paths.get(conflict.statusfile.trim());
            }
        } catch (IllegalArgumentException ex) {
            throw cmd.fail(ex.getMessage());
        }
        trayDisabled = cmd.trayDisabled();
        if (wantsTray()) {
            // Before anything touches AWT, which the tray itself is about to do
            TrayStatusIcon.configureMacOsAccessoryMode();
        }
    }

    private void showErrors() {
        status.errorCount(errors.size());
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

    /**
     * Whether a failure means the server could not be reached, as against a
     * server that answered with a refusal. Worth telling apart in a status bar:
     * the first often clears on its own when the network comes back, the second
     * needs someone to log in or fix a permission.
     */
    private static SyncState stateFor(Throwable ex) {
        Throwable t = ex;
        // bounded, because a cause chain can be made to point back at itself
        for (int i = 0; t != null && i < 20; i++) {
            if (t instanceof java.net.UnknownHostException
                    || t instanceof java.net.ConnectException
                    || t instanceof java.net.NoRouteToHostException
                    || t instanceof java.net.SocketTimeoutException) {
                return SyncState.OFFLINE;
            }
            t = t.getCause();
        }
        return SyncState.FAILED;
    }

    /**
     * @return the first exception of this type in the chain, or null. The http client wraps, so
     * the one that says what the server answered is rarely the one caught.
     */
    private static <T extends Throwable> T find(Throwable ex, Class<T> type) {
        Throwable t = ex;
        // bounded for the same reason as stateFor: a cause chain can be made to point back at itself
        for (int i = 0; t != null && i < 20; i++) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
            t = t.getCause();
        }
        return null;
    }

    /**
     * The innermost message in a failure, which is the one that says what actually went wrong.
     *
     * A refused connection arrives wrapped three deep, and the outermost layer only says that a
     * get failed.
     */
    private static String rootCauseMessage(Throwable ex) {
        Throwable t = ex;
        // bounded for the same reason as stateFor: a cause chain can be made to point back at itself
        for (int i = 0; t.getCause() != null && i < 20; i++) {
            t = t.getCause();
        }
        return StringUtils.isBlank(t.getMessage()) ? t.getClass().getSimpleName() : t.getMessage();
    }

    /**
     * An http client for a site, carrying whichever way of authenticating is in hand.
     *
     * @param user the username, which bearer auth still wants for identity and which may be null
     * when only a stored session is known
     * @param pwd a password to use, or null
     * @param cookies a saved cookie login, or null
     * @param oauth a stored OAuth2 session, or null
     */
    static Host newClient(String sRemoteAddress, String user, String pwd, Map<String, String> cookies, OAuth2Client oauth) throws MalformedURLException {
        int timeout = 180000;
        URL url = new URL(sRemoteAddress);
        Host client;
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
        client.setSecure(url.getProtocol().equals("https"));
        client.setTimeout(timeout);
        log.debug("Using timeout of " + timeout + "ms");
        client.setUseDigestForPreemptiveAuth(false);
        if (cookies != null) {
            client.getCookies().putAll(cookies);
        }
        return client;
    }

    /**
     * The failure to throw for something that went wrong while reaching the server.
     *
     * A server that could not be reached at all gets one line: the trace is http client
     * internals, and the thing to do about it - check the url, check the network - is not in
     * there. Anything else is handed back as it came, because its trace does have something to
     * say.
     *
     * @param message what was being attempted, which the reason is appended to
     */
    private static RuntimeException asFailure(Exception ex, String message) {
        if (stateFor(ex) == SyncState.OFFLINE) {
            return new SetupException(message + ": " + rootCauseMessage(ex)
                    + ". Check the url and that you are online", ex);
        }
        return ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
    }

    /**
     * Which commands publish status: the ones that talk to the server and take
     * long enough for the answer to matter. Login, logout, ignore and publish
     * have nothing a status bar would show.
     */
    private static boolean publishesStatus() {
        return "sync".equals(commandName) || "push".equals(commandName)
                || "pull".equals(commandName) || "checkout".equals(commandName);
    }

    /**
     * Only sync earns a status bar icon. The others are over in a second or
     * two, and an icon that appears and vanishes before it can be read is worse
     * than none - it would also make every short command pay for starting AWT.
     */
    private static boolean wantsTray() {
        return "sync".equals(commandName) && !trayDisabled;
    }

    /**
     * ISO-8601 in UTC. A log being read by another program should not shift
     * when the machine writing it is in a different zone, or when it crosses a
     * daylight saving boundary mid-sync.
     */
    private static final String TIMESTAMP = "%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}{UTC}";

    /**
     * The log4j2 conversion pattern for a -logformat value, or null to leave
     * the configured default (the message alone) in place.
     *
     * kv quotes the message and escapes what is inside it, because a sync
     * message can carry a file name with a quote in it and an unescaped one
     * would end the field early.
     */
    private static String logPattern(LogFormat logFormat, boolean debug) {
        if (logFormat == null) {
            // -debug on its own still wants the level and source class, as it always has.
            return debug ? "%-5p %c - %m%n" : null;
        }
        // The source class earns its place once someone is diagnosing, whichever shape they chose.
        String source = debug ? " %c" : "";
        switch (logFormat) {
            case TS:
                return TIMESTAMP + " %-5p" + source + " %m%n";
            case KV:
                return "ts=" + TIMESTAMP + " level=%p"
                        + (debug ? " source=%c" : "")
                        + " msg=\"%enc{%m}{JSON}\"%n";
            default:
                return debug ? "%-5p" + source + " - %m%n" : "%m%n";
        }
    }

    /**
     * Normal runs print the message alone; the level and class name only help
     * when something is being diagnosed, and they bury the lines a user
     * actually wants. -debug brings both back and turns the level up, and
     * -logformat asks for a shape a program can read.
     */
    private static void configureLogging(boolean debug, LogFormat logFormat) {
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

    public static void runLogin(LoginCommand cmd) throws Exception {
        log.info("Signing in..");
        KSyncUtils.withDir((File dir) -> {
            // Signing in is about a site, not about this directory: the credentials go to the
            // per-user store either way. Creating a .ksync here would leave what looks like a
            // checkout in whatever folder someone happened to be standing in.
            File repoDir = new File(dir, ".ksync");

            if (cmd.oauth()) {
                KSyncUtils.newOAuth2Client(cmd.url).login();
                return;
            }

            String user = KSync3Utils.resolve(cmd.user(), "user", KSyncUtils.USER_PROMPT);
            String pwd = KSync3Utils.getPassword(cmd.password(), user, cmd.url);

            KSync3 kSync3 = new KSync3(dir, cmd.url, user, pwd, repoDir, false, null, null);
            kSync3.login(null);
        }, cmd);
    }

    /**
     * Adds patterns to the global ignore file, or shows what is in it.
     *
     * Unlike the other commands this one is not about any one checkout, so it
     * does not need a directory, a url or a login.
     */
    /**
     * Discards the credentials for a site: the OAuth2 tokens and the cookie
     * login both, because they are two ways into the same account and leaving
     * one behind is not a logout.
     *
     * @param cmd
     * @throws java.lang.Exception
     */
    public static void logout(LogoutCommand cmd) throws Exception {
        String site = KSyncUtils.siteUrl(cmd.url);
        CredentialStore.Credentials creds = CredentialStore.defaultStore().get(site);
        boolean hadLogin = creds != null && creds.hasLogin();

        KSyncUtils.newOAuth2Client(site).logout();
        KSyncUtils.clearLogin(site);

        if (hadLogin) {
            log.info("Logged out of {}", site);
        } else {
            log.info("No stored credentials for {}, so there was nothing to discard", site);
        }
        if (StringUtils.isNotBlank(System.getenv(OAuth2Client.TOKEN_ENV_VAR))) {
            // Otherwise the next command still authenticates and the logout looks broken
            log.info("{} is still set in this environment, and will still be used", OAuth2Client.TOKEN_ENV_VAR);
        }
    }

    public static void ignore(IgnoreCommand cmd) throws Exception {
        GlobalIgnores ignores = GlobalIgnores.defaultIgnores();
        List<String> toAdd = KSync3Utils.split(cmd.pattern);

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
        log.info("Your ignore file is {}", ignores.getPath());
    }

    private static void showIgnores(GlobalIgnores ignores) {
        // Shown first, and shown even when the file is empty. A pattern nobody can see is a pattern
        // nobody can diagnose: the question these answer is "why did that not sync".
        log.info("Ignored by default, before any file is read:");
        for (String pattern : Ignores.BUILT_IN) {
            log.info("  {}", pattern);
        }
        log.info("  {} is always excluded and cannot be re-included", Ignores.STATE_DIR);
        log.info("Any of the defaults can be undone by a later rule, eg !node_modules");

        List<String> patterns = ignores.patterns();
        if (patterns.isEmpty()) {
            log.info("You have no ignore patterns yet. Add one with: ksync3 ignore --pattern \"*.log\"");
            log.info("They would be kept in {}", ignores.getPath());
        } else {
            log.info("Your patterns, from {}:", ignores.getPath());
            for (String pattern : patterns) {
                log.info("  {}", pattern);
            }
        }
        log.info("A checkout can carry its own {} too, which the whole team shares", Ignores.IGNORE_FILE);
    }

    public static final String URL_PROMPT = "the branch or repository to sync with, eg https://your-site/repositories/myrepo/version1";

    /**
     * Sets a directory up to sync with a branch, and signs in to its site, without fetching a
     * single file.
     *
     * The order is login first, then resolve: a repository will not say which version is latest
     * to someone it does not know, so asking before signing in reports a 401 in place of the
     * answer. Everything a checkout records is recorded here, so afterwards this directory is a
     * checkout in every respect except that nothing has been downloaded into it.
     *
     * @return 0 when the directory is set up, 1 when it is not
     */
    public static Integer init(InitCommand cmd) throws Exception {
        int[] exit = {0};
        KSyncUtils.withDir((File dir) -> {
            File configDir = new File(dir, ".ksync");
            String already = KSyncUtils.targetUrl(KSyncUtils.readProps(configDir));
            if (already != null) {
                reportExisting(dir, configDir, already);
                return;
            }

            String url = KSyncUtils.requireUrl(KSync3Utils.resolve(cmd.url, "url", URL_PROMPT));
            String site = KSyncUtils.siteUrl(url);
            requireReachable(url, site);
            String user = cmd.user();

            String login = storedLogin(url);
            if (login != null) {
                log.info("Already signed in to {} with {}", site, login);
            } else {
                user = signIn(dir, configDir, url, site, cmd);
                if (user == null && storedLogin(url) == null) {
                    exit[0] = 1;
                    return;
                }
            }

            exit[0] = record(dir, configDir, url, user);
        }, cmd);
        return exit[0];
    }

    /**
     * Reports a directory that is already set up, and changes nothing.
     *
     * Nothing is asked of the server for this: the question is what this directory is pointed at,
     * which is answered on disk, and a network round trip would only add a way for it to fail.
     */
    private static void reportExisting(File dir, File configDir, String target) {
        Properties props = KSyncUtils.readProps(configDir);
        log.info("{} is already set up to sync with {}", dir.getAbsolutePath(), target);
        if (StringUtils.isNotBlank(props.getProperty("repoUrl"))) {
            log.info("It follows the repository, and is currently on {}",
                    RepoMeta.versionNameOf(props.getProperty("url")));
        }
        String login = storedLogin(target);
        if (login == null) {
            log.info("There is no stored login for {}. Run: ksync3 login --url {}", KSyncUtils.siteUrl(target), target);
        } else {
            log.info("Signed in to {} with {}", KSyncUtils.siteUrl(target), login);
        }
        log.info("Nothing to change. To point it somewhere else, remove {} first",
                new File(configDir, "ksync.properties").getAbsolutePath());
    }

    /**
     * Checks the site is there at all, before anyone is asked to type a password into it.
     *
     * A refusal is a fine answer: it means the server is up and wants a login, which is the next
     * thing this does. What this is for is the url that goes nowhere - a name typed wrongly, a
     * vpn that is not up - which otherwise is reported only after a username and a password have
     * been typed in after it.
     */
    private static void requireReachable(String url, String site) throws MalformedURLException {
        Host client = newClient(url, null, null, null, null);
        try {
            client.get(RepoMeta.withTrailingSlash(new URL(url).getFile()) + "?type=hash");
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException | NotFoundException ex) {
            log.debug("{} answered {} without a login, which is not an answer to whether it is there", url, ex.toString());
        } catch (RuntimeException ex) {
            throw asFailure(ex, "Could not reach " + site);
        }
    }

    /**
     * How this machine would authenticate to a site, as a phrase to print, or null when it has no
     * way to.
     */
    private static String storedLogin(String url) {
        if (StringUtils.isNotBlank(System.getenv(OAuth2Client.TOKEN_ENV_VAR))) {
            return "the " + OAuth2Client.TOKEN_ENV_VAR + " set in this environment";
        }
        if (KSyncUtils.oauth2SessionOrNull(url) != null) {
            return "an OAuth2 session";
        }
        Map<String, String> cookies = KSyncUtils.getCookies(url);
        if (cookies.isEmpty()) {
            return null;
        }
        String userUrl = cookies.get("miltonUserUrl");
        // Only the readable shape. The other one a server sends is base64 with a b64 prefix, and
        // a line of that tells whoever reads it nothing about who they are signed in as.
        return userUrl != null && userUrl.startsWith("/users/")
                ? "a saved login as " + StringUtils.strip(userUrl.substring("/users/".length()), "/")
                : "a saved login";
    }

    /**
     * Signs in to a site, by browser where the site offers it and by password where it does not.
     *
     * A browser first because it is the one that needs no password typed and expires on its own,
     * and because the fallback costs nothing: a site without OAuth2 has no metadata document, so
     * it says so in one request. Naming a user with -u says which way is wanted and skips
     * straight to the password.
     *
     * @return the username signed in as, or null - which is not a failure on its own, because an
     * OAuth2 login knows the site rather than the name
     */
    private static String signIn(File dir, File configDir, String url, String site, InitCommand cmd) throws Exception {
        if (cmd.user() == null) {
            try {
                KSyncUtils.newOAuth2Client(url).login();
                log.info("Signed in to {}", site);
                return null;
            } catch (IOException | RuntimeException ex) {
                if (cmd.oauth()) {
                    throw new SetupException("Could not sign in to " + site + " with a browser: "
                            + rootCauseMessage(ex), ex);
                }
                log.info("Could not sign in to {} with a browser: {}", site, rootCauseMessage(ex));
                log.info("Falling back to a username and password");
            }
        }
        String user = KSync3Utils.resolve(cmd.user(), "user", KSyncUtils.USER_PROMPT);
        String pwd = KSync3Utils.getPassword(cmd.password(), user, url);
        new KSync3(dir, url, user, pwd, configDir, false, null, null).login(null);
        if (storedLogin(url) == null) {
            log.error("Could not sign in to {} as {}, so nothing has been set up here", site, user);
            return null;
        }
        log.info("Signed in to {} as {}", site, user);
        return user;
    }

    /**
     * Works out what the url names, checks the branch answers, and records it.
     *
     * The check is the same request a push makes, so a url which is a typo, or a version this
     * login cannot read, is reported now rather than by the first sync. Nothing is recorded when
     * it fails: a .ksync naming a branch that does not answer is worse than no .ksync at all,
     * because every later command in this directory would take its url from it.
     *
     * @return the exit code
     */
    private static Integer record(File dir, File configDir, String url, String user) throws Exception {
        Host client = newClient(url, user, null, KSyncUtils.getCookies(url), KSyncUtils.oauth2SessionOrNull(url));
        RepoMeta meta = RepoMeta.fetch(client, url);
        String versionUrl = url;
        String repoUrl = null;
        if (meta != null) {
            RepoMeta.Version latest = meta.getLatestVersion();
            if (latest == null) {
                throw new SetupException("No version of " + url + " has a version number for a name,"
                        + " so there is no latest one to follow. It has: " + String.join(", ", meta.versionNames())
                        + ". Give the url of one of those to pin this directory to it");
            }
            repoUrl = url;
            versionUrl = RepoMeta.versionUrl(url, latest.getName());
            log.info("{} is a repository. Its latest version is {}{}", url, latest.getName(),
                    meta.getLiveVersion() == null ? "" : ", and the live one is " + meta.getLiveVersion().getName());
        }

        String hash = branchHash(client, versionUrl);
        if (hash == null) {
            log.error("{} did not answer with a branch hash, so it is not a version this login can sync with."
                    + " Nothing has been set up here", versionUrl);
            return 1;
        }
        log.debug("Branch {} is at {}", versionUrl, hash);

        configDir.mkdirs();
        KSyncUtils.writeProps(versionUrl, user, repoUrl, configDir);
        log.info("{} now syncs with {}", dir.getAbsolutePath(), versionUrl);
        if (repoUrl != null) {
            log.info("It follows the repository, so it moves to each new version as one is published");
        }
        // Said plainly, because this is the one way init differs from checkout, and a directory
        // holding nothing is otherwise indistinguishable from a checkout that has been emptied
        log.info("Nothing has been downloaded. Run: ksync3 pull to bring the branch down here,"
                + " or ksync3 sync to push what is already here as it changes");
        return 0;
    }

    /**
     * @return the branch hash at a url, or null when it does not answer with one
     */
    private static String branchHash(Host client, String versionUrl) throws MalformedURLException {
        String path = RepoMeta.withTrailingSlash(new URL(versionUrl).getFile());
        try {
            byte[] resp = client.get(path + "?type=hash");
            if (resp == null) {
                return null;
            }
            String s = new String(resp).trim();
            return HASH.matcher(s).matches() ? s : null;
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException | NotFoundException ex) {
            log.debug("Asked {} for a branch hash and it refused", path, ex);
            return null;
        }
    }

    public static void checkout(CheckoutCommand cmd) throws Exception {
        log.info("Checking out..");

        KSyncUtils.withKsync((KSync3 kSync3) -> {
            kSync3.checkout(kSync3.repoDir);
            kSync3.showErrors();
        }, cmd, false);

    }

    public static void push(PushCommand cmd) throws Exception {
        log.info("Pushing local changes..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            log.debug("do push {}", configDir);
            k.push(configDir);
            k.showErrors();
        }, cmd, false);
        System.exit(0); // threads arent shutting down
    }

    public static void sync(SyncCommand cmd) throws Exception {
        log.info("Syncing..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            k.start();
            k.showErrors();
        }, cmd, true);
        log.info("Finished the initial scan");

    }

    public static void pull(PullCommand cmd) throws Exception {
        log.info("Pulling changes from the server..");
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            try {
                k.pull(configDir);
                k.showErrors();
            } catch (IOException ex) {
                log.error("ex", ex);
            }
        }, cmd, false);
        log.info("Done");
        System.exit(0); // threads arent shutting down
    }

    /**
     * Asks the server which files in this version are missing objects, and
     * reports them.
     *
     * Exits 1 when anything is missing, so a script or an assistant can act on
     * the answer without reading the output. This reports a fault in the
     * version, not a fault in the command, so it has to be distinguishable from
     * a clean check.
     */
    public static void verify(VerifyCommand cmd) throws Exception {
        int[] missing = new int[]{0};
        KSyncUtils.withKSync((File configDir, KSync3 k) -> {
            missing[0] = k.verify();
        }, cmd, false);
        System.exit(missing[0] > 0 ? 1 : 0);
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
    private final Ignores ignores;
    private final SyncHashCache fileHashCache;
    private final FileSystemWatchingService fileSystemWatchingService;
    private final ScheduledExecutorService scheduledExecutorService;

    private final List<String> errors = new ArrayList<>();

    /**
     * The url of the version being synced, which is what every request here is
     * relative to.
     */
    private final String remoteAddress;

    /**
     * The repository url this checkout follows, or null when it is pinned to
     * one version.
     *
     * When set, remoteAddress was resolved from it on startup and is the latest
     * version at that moment, rather than something a person chose once and has
     * to maintain by hand.
     */
    private final String trackedRepoUrl;

    /**
     * The metadata read while resolving the version, or null when this checkout
     * is pinned.
     */
    private RepoMeta repoMeta;

    /**
     * Publishes what this sync is doing to the status file and, for the sync
     * command, the OS status bar. Never null - the commands with nothing to say
     * get a reporter that goes nowhere.
     */
    private final SyncStatusReporter status;

    /**
     * Whether the sync got as far as watching for changes.
     *
     * Until it has, a failed push is a failure to start, and is treated as one. After it has, the
     * sync is up and the same failure is something to wait out. See {@link #pushFailed}.
     */
    private volatile boolean watching;

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, Ignores ignores, Map<String, String> cookies) throws MalformedURLException, IOException {
        this(localDir, sRemoteAddress, user, pwd, configDir, background, ignores, cookies, null);
    }

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, Ignores ignores, Map<String, String> cookies, OAuth2Client oauth) throws MalformedURLException, IOException {
        this(localDir, sRemoteAddress, user, pwd, configDir, background, ignores, cookies, oauth, RepoMeta.Tracking.OFF);
    }

    public KSync3(File localDir, String sRemoteAddress, String user, String pwd, File configDir, boolean background, Ignores ignores, Map<String, String> cookies, OAuth2Client oauth, RepoMeta.Tracking tracking) throws MalformedURLException, IOException {
        this.localDir = localDir;
        this.configDir = configDir;
        // Never null: a fetch walks it for every child, and login builds one of these with none
        this.ignores = ignores == null ? Ignores.none() : ignores;
        eventManager = new EventManagerImpl();

        client = newClient(sRemoteAddress, user, pwd, cookies, oauth);

        // Which version to work against. Only now, because asking the repository needs the client,
        // and everything below is relative to the answer.
        //
        // Asked once, here, and held for the life of the process. A long running sync deliberately
        // does not watch for a newer version appearing: publishing one is something a developer
        // does, at a moment of their choosing, so they already know it happened and can restart the
        // sync. Switching version underneath a running sync would mean moving the working copy
        // while file watches are live, which is a real risk taken in exchange for nothing.
        this.trackedRepoUrl = trackedRepoUrl(tracking, sRemoteAddress);
        this.remoteAddress = trackedRepoUrl == null ? sRemoteAddress : latestVersionUrl(trackedRepoUrl);
        this.status = publishesStatus()
                ? SyncStatusReporter.create(commandName, localDir, configDir, remoteAddress, statusFile, wantsTray())
                : SyncStatusReporter.none();
        branchPath = new URL(remoteAddress).getFile();

        repoDir = new File(localDir, ".ksync");

        // Keyed on the repository when following one, not on the version, so two checkouts of one
        // repository share their objects instead of each fetching the same blob. The file hash
        // cache below is keyed the same way, for the same reason.
        String cacheKey = trackedRepoUrl == null ? remoteAddress : trackedRepoUrl;

        // Not under the checkout: both stores fan a hash out over nested directories, one small
        // file per object, and tens of thousands of them inside the folder being worked in are
        // indexed by editors and copied by other sync tools. Checkouts made before this moved
        // bring theirs with them.
        File objectsDir = ObjectStoreDir.forRepo(cacheKey);
        ObjectStoreDir.migrate(repoDir, objectsDir);
        this.localBlobStore = new FileSystem2BlobStore(new File(objectsDir, ObjectStoreDir.BLOBS));
        this.localHashStore = new FileSystem2HashStore(new File(objectsDir, ObjectStoreDir.HASHES));

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
        // Same key as the object store above: the cache is about local files, so it stays valid
        // across a version change, and rebuilding it on every release would be a slow scan of the
        // whole checkout for nothing.
        File envDir = new File(tmpDir, "appDeployer-filecache-" + KSync3Utils.makeFileName(cacheKey));
        try {
            fileHashCache = new BerkeleyDbFileHashCache(envDir);
        } catch (RuntimeException ex) {
            // One writer at a time, and the raw failure names a path under /tmp without ever
            // mentioning the other process holding it. Note the key is the repository when
            // following one, so two checkouts of different versions of it collide here too.
            if (ex.getClass().getSimpleName().contains("EnvironmentLocked")) {
                throw new SetupException("Another ksync3 is already working on " + cacheKey
                        + ". Stop it, usually a sync running in another terminal, and try again."
                        + " Two checkouts of the same repository share this cache, so a sync in either blocks the other.", ex);
            }
            throw ex;
        }

        tripletStore = new MemoryLocalTripletStore(localDir, eventManager, localBlobStore, localHashStore, (String rootHash) -> {
            if (background) {
                try {
                    log.debug("File changed in {}, new repo hash {}", localDir, rootHash);
                    push(rootHash, configDir);

                } catch (Exception ex) {
                    pushFailed(ex);
                }
            }
        }, null, fileSystemWatchingService, ignores, fileHashCache);
//        MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), blobStore, hashStore, (String rootHash) -> {
//            needsPush.set(true);
//        }, null, fileWatchService, null, fileHashCache);

    }

    /**
     * Reports a background push that failed, and ends the sync when it can only
     * fail again.
     *
     * A sync is built to outlive a bad push: the server may be down, or a pull
     * may be needed, and the next change is worth trying. An expired login is
     * not like that. Nothing this process can do will renew it, so every later
     * push fails identically, and a sync still running while saving nothing is
     * worse than one that stopped - whoever is editing files has no reason to
     * suspect their work is going nowhere.
     *
     * No stack trace for that one: it is understood, and the message names the
     * single thing to do about it. This is the same treatment the one-shot
     * commands get in {@link #handleKSync}.
     */
    private void pushFailed(Exception ex) {
        if (!reportPushFailure(status, ex, watching)) {
            return;
        }
        // Left in a state that says what happened, because this runs on a watch thread with
        // nobody at the terminal: the status file and the notification are how it gets noticed
        status.stopped();
        status.close();
        System.exit(1);
    }

    /**
     * Logs and records a failed push.
     *
     * @param watching whether the sync is up and watching for changes, which is
     * what makes waiting for the next change a sensible thing to do
     * @return true when the sync cannot continue, so the process should end
     */
    static boolean reportPushFailure(SyncStatusReporter status, Exception ex, boolean watching) {
        NotLoggedInException notLoggedIn = NotLoggedInException.find(ex);
        if (notLoggedIn == null) {
            log.error("Exception in file changed event handler", ex);
            status.problem(stateFor(ex), "Push failed: " + ex.getMessage());
            if (watching) {
                return false;
            }
            // This is the push the initial scan asked for, and it failed before the sync was
            // watching anything, so there is no next change to wait for and nothing has ever
            // worked. Whoever ran the command is still at the terminal to read why, which will
            // not be true of a failure an hour from now.
            log.error("That was the first push, so the sync never started. Stopping,"
                    + " rather than sitting here watching a checkout it has not once been able to push.");
            return true;
        }
        log.error(notLoggedIn.getMessage());
        status.problem(SyncState.FAILED, notLoggedIn.getMessage());
        return true;
    }

    /**
     * The repository url this checkout follows, or null when it is pinned to a
     * version.
     *
     * A probe is optimistic: if the server cannot be reached to answer it,
     * carry on with the url as given rather than failing here. Whatever the
     * command does next will report the real problem, and it will report it
     * better than a question about metadata would.
     */
    private String trackedRepoUrl(RepoMeta.Tracking tracking, String sRemoteAddress) throws IOException {
        if (tracking == RepoMeta.Tracking.OFF) {
            return null;
        }
        try {
            repoMeta = RepoMeta.fetch(client, sRemoteAddress);
        } catch (IOException | RuntimeException ex) {
            // RuntimeException as well as IOException, because a refused connection comes back
            // from the http client wrapped in one, and without this the probe was optimistic in
            // name only: the commonest failure of all escaped from here as a trace through the
            // http client, before the command had said anything about what it was trying to do.
            if (tracking != RepoMeta.Tracking.TRACK) {
                log.debug("Could not check whether {} is a repository, treating it as a version", sRemoteAddress, ex);
                return null;
            }
            // it is known to be a repository, so there is no version to fall back to
            throw asFailure(ex, "Could not reach " + sRemoteAddress + " to check which version to sync with");
        }
        if (repoMeta == null) {
            if (tracking == RepoMeta.Tracking.TRACK) {
                throw new SetupException(sRemoteAddress + " no longer answers as a repository."
                        + " Point -url at a version to pin this checkout to one");
            }
            return null;
        }
        return sRemoteAddress;
    }

    /**
     * The url of the latest version of the tracked repository, reporting which
     * version that is and whether it has moved since this checkout last ran.
     */
    private String latestVersionUrl(String repoUrl) {
        RepoMeta.Version latest = repoMeta.getLatestVersion();
        if (latest == null) {
            throw new SetupException("No version of " + repoUrl + " has a version number for a name,"
                    + " so there is no latest one to follow. It has: " + String.join(", ", repoMeta.versionNames())
                    + ". Point -url at one of those to pin this checkout to it");
        }
        String url = RepoMeta.versionUrl(repoUrl, latest.getName());
        String previous = KSyncUtils.readProps(configDir).getProperty("url");
        if (previous != null && !previous.equals(url)) {
            // Not a debug line: this changes which version the next push writes to
            log.info("Version {} is now the latest of {}, moving this checkout from {}",
                    latest.getName(), repoUrl, RepoMeta.versionNameOf(previous));
        } else {
            log.info("Version {} is the latest of {}", latest.getName(), repoUrl);
        }
        return url;
    }

    private void start() throws MalformedURLException, IOException {
        status.state(SyncState.STARTING, "checking the connection");
        checkRemote();
        log.info("Do initial scan");
        status.state(SyncState.SCANNING, "initial scan");
        tripletStore.scan();
        log.info("Done initial scan, now begin monitoring..");
        tripletStore.start();
        log.debug("Done monitor init");
        watching = true;
        status.ready("watching for local changes");
    }

    /**
     * Asks the branch for its hash before any watching begins, so a sync that cannot work says so
     * now instead of running on.
     *
     * A sync spends its life waiting for a file to change, so a broken connection has nothing to
     * break against until someone saves a file, and if the initial scan finds nothing to push that
     * may be hours away. Until then the process looks exactly like a working sync: still running,
     * no error since the one at startup, an icon in the status bar. One request up front is the
     * difference between that and a command that exits with the reason.
     *
     * This is the request every push makes anyway, so it costs a sync nothing it was not about to
     * spend, and it exercises the whole path: the url, the network, and the login.
     */
    private void checkRemote() {
        String remoteHash;
        try {
            remoteHash = getRemoteHash(branchPath);
        } catch (RuntimeException ex) {
            if (find(ex, NotAuthorizedException.class) != null) {
                throw new SetupException(remoteAddress + " refused the login for " + client.user
                        + ". Check the username and password, or run: ksync3 login", ex);
            }
            throw asFailure(ex, "Could not reach " + remoteAddress + " to start the sync");
        }
        if (remoteHash == null) {
            throw new SetupException(remoteAddress + " did not answer with a hash for this branch,"
                    + " so there is nothing to sync with. Check the url names a version of a repository");
        }
        log.info("Connected to {}", remoteAddress);
    }

    /**
     * Signs in with the password this instance was built with, and stores the
     * session cookie the server hands back so the next command does not need
     * the password.
     *
     * @param secondFactor a 2FA code, or null on the first attempt
     */
    private void login(String secondFactor) {
        login(secondFactor, true);
    }

    /**
     * @param mayAskFor2FA whether a second factor can be prompted for. True for
     * the login command, where someone is waiting at a terminal for exactly
     * this. False when saving a session on the way into another command: a sync
     * run from cron or a desktop launcher has nobody to answer, and would sit
     * on a stdin that never produces a line
     */
    private void login(String secondFactor, boolean mayAskFor2FA) {
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
                    if (!mayAskFor2FA) {
                        log.info("Could not save a session for {}: the server asked for a second factor."
                                + " Run: ksync3 login", client.server);
                        break;
                    }
                    log.info("Authentication failed. Is 2FA required?");
                    String s = KSync3Utils.getInput("2FA code");
                    if (StringUtils.isNotBlank(s)) {
                        login(s, mayAskFor2FA);
                    } else {
                        log.info("Login aborted");
                    }
                    break;
                case 400:
                    log.info("Authentication failed. Is your userid correct?");
                    break;
                case 200:
                    log.debug("login: completed {}", res);
                    String userUrlHash = userUrlHashFrom(result.getHeaders());
                    if (userUrlHash == null) {
                        log.warn("Login seemed to succeed, but didnt find an authorisation cookie");
                    } else {
                        KSyncUtils.writeLoginProps("/users/" + this.client.user + "/", userUrlHash, this.remoteAddress);
                    }
                    break;

                default:
                    log.warn("login: unhandled result code: {}", res);
                    break;
            }
        } catch (IOException ex) {
            if (!mayAskFor2FA) {
                // Best effort, from saveLogin, which says what to make of it. A stack trace here
                // is noise in front of whatever the command itself is about to report, and it
                // reads like the reason the command failed when it is not.
                throw new RuntimeException(ex);
            }
            log.error("login: exception occured", ex);
        }
    }

    /**
     * Saves a session for the password this instance was built with, so the
     * next command runs without asking for it.
     *
     * Only worth calling when a password was actually used. Failing is not the
     * command's failure: the password in hand authenticates every request
     * either way, so whatever was asked for goes ahead, and the only cost is
     * being asked for the password again next time.
     */
    void saveLogin() {
        log.debug("Trading the password for a session, so it is not needed next time");
        try {
            login(null, false);
        } catch (RuntimeException ex) {
            log.warn("Could not save a session for {}, so the password will be needed again next time: {}",
                    remoteAddress, rootCauseMessage(ex));
        }
    }

    /**
     * Reads the milton auth cookie out of a login response.
     *
     * Servers return these in two shapes - one Set-Cookie header per cookie,
     * and every cookie on a single line - so this looks for the cookie by name
     * anywhere in the value rather than expecting it in a particular place:
     *
     * <pre>
     * miltonUserUrl=b64L3VzZXJzL2JyYWQv; Path=/; Expires=Wed, 04-Sep-2019 23:59:47 GMT
     * miltonUserUrlHash="YYY-XXX-YYY-ZZZ-XXX:DDDD"; Path=/; Expires=Sat, 24-Aug-2019 02:29:54 GMT; HttpOnly
     * </pre>
     *
     * @return the userUrlHash cookie value, or null when the response carries
     * no such cookie
     */
    static String userUrlHashFrom(Map<String, String> headers) {
        if (headers == null) {
            return null;
        }
        String setCookie = headers.get("Set-Cookie");
        if (StringUtils.isBlank(setCookie)) {
            return null;
        }
        String key = "miltonUserUrlHash=\"";
        int pos = setCookie.indexOf(key);
        if (pos < 0) {
            return null;
        }
        String rest = setCookie.substring(pos + key.length());
        int end = rest.indexOf('"');
        if (end < 0) {
            return null; // unterminated, so there is no value to be read out of it
        }
        // deliberately not logged, and not put in any exception: this is the auth hash itself
        return rest.substring(0, end);
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
        status.state(SyncState.PUSHING, "checking the remote");
        String remoteHash = getRemoteHash(branchPath);
        if (remoteHash == null) {
            log.info("Aborted");
            status.problem(SyncState.OFFLINE, "The server did not return a hash for " + branchPath);
            return;
        }
        status.hashes(localRootHash, remoteHash);
        if (remoteHash.equals(localRootHash)) {
            log.info("No change. Local repo is exactly the same as remote hash={}", localRootHash);
            status.state(SyncState.IDLE, "nothing to push");
            return;
        }

        String lastRemoteHash = KSyncUtils.getLastRemoteHash(configDir);
        if (!remoteHash.equals(lastRemoteHash)) {
            if (!localWins) {
                log.info("Remote repository has changed, please pull. Current remote={} last remote={}", remoteHash, lastRemoteHash);
                status.problem(SyncState.BLOCKED, "The remote has changed. Pull, or use -localwins to overwrite it");
                return;
            }
            // Still worth a line: this is the point at which remote-only changes are lost, so
            // the hash that was on the server needs to be in the log to go back to.
            log.info("Remote repository has changed, overwriting it from local because -localwins was given. Current remote={} last remote={}", remoteHash, lastRemoteHash);
            // Notified, not just logged. Discarding someone else's push is exactly the kind of
            // thing that should not happen silently behind an assistant driving the sync.
            status.alert("ksync: overwriting the remote",
                    "The remote had changed and -localwins replaced it from local. Previous remote hash " + remoteHash, false);
            status.state(SyncState.PUSHING, "overwriting a changed remote");
        }

        // walk the VFS and push hashes and blobs to the remote store. Anything
        // already in the remote store will be ignored
        status.state(SyncState.PUSHING, "uploading changed files");
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
                    status.hashes(localRootHash, localRootHash);
                    status.errorCount(errors.size());
                    status.state(SyncState.IDLE, "pushed");
                    return;
                }
            }
            log.warn("Push failed: Check for missing objects", res);
            status.state(SyncState.PUSHING, "uploading objects the server was missing");
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
            status.problem(SyncState.FAILED, "Could not set the repository hash: " + ex.getMessage());
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
     * @param synchronous when true the writes happen on this thread. Writing to
     * the remote is done on the transfer executor so uploads overlap, and the
     * file fanout must not be set until they finish, which is what the wait
     * below is for. Writing to the local stores is just disk IO - the expensive
     * part, fetching from the source, has already happened on this thread - so
     * queueing it buys nothing and the wait would cost a second per file.
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
        status.state(SyncState.PULLING, "checkout");
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
        pull(hash, this.localDir, "", ignores); // pull from local blobstore into local vfs
        KSyncUtils.saveRemoteHash(configDir, hash);
        log.info("finished checkout");
        status.hashes(hash, hash);
        status.errorCount(errors.size());
        status.state(SyncState.IDLE, "checked out");
    }

    /**
     * Returns the new local hash
     *
     * @param configDir
     * @return
     * @throws IOException
     */
    public String pull(File configDir) throws IOException {
        status.state(SyncState.PULLING, "checking the remote");
        String localHash = commit();
        String lastRemoteHash = KSyncUtils.getLastRemoteHash(configDir);
        String remoteHash = getRemoteHash(branchPath);
        status.hashes(localHash, remoteHash);
        if (lastRemoteHash != null && lastRemoteHash.equals(remoteHash)) {
            log.info("No change on server since last pull");
            status.state(SyncState.IDLE, "nothing to pull");
            return null;
        }
        status.state(SyncState.PULLING, "fetching changes");
        try {
            fetch(Path.root, remoteHash, ignores); // fetch into local blobstore
        } catch (InterruptedException ex) {
            log.error("interripted", ex);
            return null;
        }

        DeltaGenerator dg = new DeltaGenerator(wrappedHashStore, wrappedBlobStore, new FileUpdatingMergingDeltaListener(localDir, httpHashStore, httpBlobStore,
                ConflictResolvers.create(conflictMode, localWins)));
        dg.generateDeltas(lastRemoteHash, remoteHash, localHash); // calc changes and apply them to the working directory

        log.info("Finished pull, save hash " + remoteHash);
        KSyncUtils.saveRemoteHash(configDir, remoteHash);
        String newLocalHash = commit();
        status.hashes(newLocalHash, remoteHash);
        status.errorCount(errors.size());
        status.state(SyncState.IDLE, "pulled");
        return newLocalHash;
    }

    /**
     * Runs the whole-branch missing object check on the server and reports what
     * it finds.
     *
     * The walk is the server's, because only the server knows what is in its
     * stores. It is not cheap on a large branch, hence the line saying it has
     * started.
     *
     * @return the number of missing objects
     */
    private int verify() throws IOException {
        log.info("Checking {} for missing objects, this walks the whole version..", remoteAddress);
        Map<String, String> params = new HashMap<>();
        params.put("findMissingObjects", "true");
        String res;
        try {
            res = client.post(branchPath, params);
        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new IOException("Could not run the missing object check: " + ex.getMessage(), ex);
        }
        MissingObjects missing = MissingObjects.parse(res);
        log.info("");
        for (String reportLine : missing.report()) {
            log.info(reportLine);
        }
        return missing.getObjects().size();
    }

    /**
     * A branch hash is hex and nothing else. Anything else means the url is not a branch, and the
     * commonest way to get here is pasting the address of the admin page for a website, which
     * answers with that page. Without this check the page itself became the hash and went into the
     * next request's path, which came back as a 414 with the whole document quoted in the message.
     */
    private static final java.util.regex.Pattern HASH = java.util.regex.Pattern.compile("[0-9a-fA-F]{8,64}");

    private String getRemoteHash(String path) {
        try {
            byte[] resp = client.get(path + "/?type=hash");
            if (resp == null) {
                return null;
            }
            String s = new String(resp).trim();
            if (!HASH.matcher(s).matches()) {
                log.debug("Asked {} for a branch hash and got {} bytes of something else", path, s.length());
                throw new SetupException(remoteAddress + " is not a repository branch: it answered with a page"
                        + " rather than a version. A checkout url looks like"
                        + " https://yoursite.kademi.co/repositories/<repository>/<version>/");
            }
            return s;
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException | NotFoundException ex) {
            status.problem(stateFor(ex), "Could not read the remote hash: " + ex.getMessage());
            throw new RuntimeException(ex);
        } catch (RuntimeException ex) {
            // The one that actually happens. A refused connection is wrapped in a
            // RuntimeException inside client.get and comes out here, not as one of the checked
            // exceptions above, so catching only those left the status saying "checking the
            // remote" after the command had already given up.
            status.problem(stateFor(ex), "Could not read the remote hash: " + ex.getMessage());
            throw ex;
        }
    }

    /**
     *
     *
     * @param hash
     */
    private void fetch(Path filePath, String hash, Ignores ignores) throws InterruptedException {
        try {
            startFileDownloads();
            _fetch(filePath, "", hash, ignores);
            log.debug("Waiting for file downloads to finish.");
            while (!areDownloadsFinished()) {
                Thread.sleep(500);
            }
        } finally {
            stopFileDownloads();
        }
        log.debug("fetch finished");
    }

    /**
     * @param relPath where this directory sits below the branch root, which is what an ignore rule
     * is matched against. Empty at the root.
     */
    private void _fetch(Path filePath, String relPath, String hash, Ignores ignores) throws InterruptedException {
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
                String childPath = relPath.isEmpty() ? t.getName() : relPath + "/" + t.getName();
                if (!ignores.ignored(childPath, t.getType().equals("d"))) {
                    if (t.getType().equals("d")) {
                        _fetch(filePath.child(t.getName()), childPath, t.getHash(), ignores);
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

    private void pull(String hash, File dir, String relPath, Ignores ignores) {
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
                String childPath = relPath.isEmpty() ? t.getName() : relPath + "/" + t.getName();
                if (!ignores.ignored(childPath, t.getType().equals("d"))) {
                    if (t.getType().equals("d")) {
                        File dir2 = new File(dir, t.getName());
                        dir2.mkdirs();
                        pull(t.getHash(), dir2, childPath, ignores);
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

    /**
     * @return the url of the version being synced, which is the resolved one
     * when following a repository
     */
    public String getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * @return the repository url being followed, or null when this checkout is
     * pinned to a version
     */
    public String getTrackedRepoUrl() {
        return trackedRepoUrl;
    }

    public String getBranchPath() {
        return branchPath;
    }

    public Ignores getIgnores() {
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
            status.problem(stateFor(ex), "Push failed: " + ex.getMessage());
        } catch (InterruptedException ex) {
            log.warn("Interrupted", ex);
            status.state(SyncState.STOPPED, "interrupted");
        } catch (RuntimeException ex) {
            // A net under everything else that can fail hard mid push. Without it the status is
            // left mid operation and the shutdown hook turns that into a bare "stopped", which
            // reads as though the push simply finished.
            status.problem(stateFor(ex), "Push failed: " + ex.getMessage());
            throw ex;
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
