package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import co.kademi.sync.oauth.OAuth2Client;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import net.sf.json.JSONArray;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class KSyncUtils {

    private static final Logger log = LoggerFactory.getLogger(KSyncUtils.class);

    public static void withKsync(CheckedConsumer<KSync3> command, Options options, CommandLine line, boolean needsUrl, boolean background) throws Exception {
        KSyncUtils.withDir((File dir) -> {
            File configDir = new File(dir, ".ksync");
            configDir.mkdirs();
            Properties props = KSyncUtils.readProps(configDir);

            String url = requireUrl(KSync3Utils.getInput(options, line, "url", props, needsUrl), dir, options, line);
            migrateLegacyCredentials(url, configDir);

            String auth = line.getOptionValue("auth");
            if (StringUtils.isNotBlank(auth)) {
                if (auth.contains(",")) {
                    String[] arr = auth.split(",");
                    String userName = arr[0].trim();
                    String token = arr[1].trim();
                    String userUrl = "/users/" + userName;
                    log.debug("Auth token provided in args: userUrl={}", userUrl);
                    writeLoginProps(userUrl, token, url);
                }
            }
            Map cookies = KSyncUtils.getCookies(url);
            OAuth2Client oauth = oauth2SessionOrNull(url);
            String user = KSync3Utils.getInput(options, line, "user", props, oauth == null && cookies.isEmpty());
            String pwd = null;
            if (oauth != null) {
                log.debug("Using the stored OAuth2 session, so dont prompt for password");
            } else if (cookies.isEmpty()) {
                pwd = KSync3Utils.getPassword(line, user, url);
            } else {
                log.debug("We have a saved login, so dont prompt for password: User={}", cookies.get("miltonUserUrl"));
            }
            String sIgnores = KSync3Utils.getInput(options, line, "ignore", props, false);
            List<String> ignores = GlobalIgnores.combine(KSync3Utils.split(sIgnores));
            KSyncUtils.writeProps(url, user, configDir);

            KSync3 kSync3 = new KSync3(dir, url, user, pwd, configDir, background, ignores, cookies, oauth);
            command.accept(kSync3);
        }, options, line);
    }

    /**
     * The url of the remote branch this directory syncs with. Every command needs one: without it
     * there is nothing to talk to, and the failure surfaced as a MalformedURLException from deep
     * inside the KSync3 constructor, which says nothing about what to do next.
     *
     * A directory with no ksync.properties is not a checkout yet, so ask for the url when there is
     * someone at a terminal to answer. A background sync has nobody to ask, so tell it plainly
     * rather than blocking forever on a stdin that will never produce a line.
     */
    static String requireUrl(String url, File dir, Options options, CommandLine line) {
        if (StringUtils.isBlank(url)) {
            if (System.console() == null) {
                throw new SetupException(dir.getAbsolutePath() + " is not a ksync checkout, and there is no terminal to ask for the url."
                        + " Run it again with -url https://your-site/repo/branch, or check the branch out here first");
            }
            log.info("{} is not a ksync checkout yet, so there is no url to sync with", dir.getAbsolutePath());
            url = KSync3Utils.getInput(options, line, "url", null, true);
            if (StringUtils.isBlank(url)) {
                throw new SetupException("No url given, so there is nothing to sync with");
            }
        }
        url = url.trim();
        try {
            new java.net.URL(url);
        } catch (java.net.MalformedURLException ex) {
            throw new SetupException("Not a valid url: " + url + " - it should look like https://your-site/repo/branch", ex);
        }
        return url;
    }

    public static void withDir(CheckedConsumer<File> s, Options options, CommandLine line) throws Exception {
        String curDir = KSync3Utils.getOrCreateAppDirectory(line);
        File dir = new File(curDir);

        if (!dir.exists()) {
            log.error("Directory does not exist: " + dir.getAbsolutePath());
        } else {
            if (!dir.isDirectory()) {
                log.error("Is not a directory: " + dir.getAbsolutePath());
            } else {
                s.accept(dir);
            }
        }
    }

    public static void withKSync(KSyncCommand c, CommandLine line, Options options, boolean backgroundSync) throws Exception {
        withKsync((KSync3 kSync3) -> {
            c.accept(kSync3.getConfigDir(), kSync3);
        }, options, line, false, backgroundSync);
//
//        KSyncUtils.withDir((File dir) -> {
//            File configDir = new File(dir, ".ksync");
//            Properties props = KSyncUtils.readProps(configDir);
//            String url = props.getProperty("url");
//            String user = props.getProperty("user");
//            String sIgnore = props.getProperty("ignore");
//            List<String> ignores = split(sIgnore);
//
//            String pwd = line.getOptionValue("password");
//            if (StringUtils.isBlank(pwd)) {
//                Console con = System.console();
//                if (con != null) {
//                    char[] chars = con.readPassword("Enter your password for " + user + "@" + url + ":");
//                    pwd = new String(chars);
//                } else {
//                    Scanner scanner = new Scanner(System.in);
//                    System.out.println("Enter your password for " + user + "@" + url + ":");
//                    pwd = scanner.next();
//                }
//            }
//
//            try {
//                Map<String, String> cookies = getCookies(props);
//                KSync3 kSync3 = new KSync3(dir, url, user, pwd, configDir, backgroundSync, ignores, cookies);
//                c.accept(configDir, kSync3);
//            } catch (IOException ex) {
//                System.out.println("Ex: " + ex.getMessage());
//            }
//        }, options);
    }

    private static List<String> split(String sIgnore) {
        List<String> list = new ArrayList<>();
        if (StringUtils.isNotBlank(sIgnore)) {
            for (String s : sIgnore.split(",")) {
                list.add(s.trim());
            }
        }
        return list;
    }

    /**
     * An OAuth2 client for a site. Credentials live in the per-user store, not in the repo, so
     * a token is never sitting in a directory a git commit can reach.
     */
    public static OAuth2Client newOAuth2Client(String url) {
        return new OAuth2Client(baseUrlOf(url), CredentialStore.defaultStore());
    }

    /**
     * @return an OAuth2 client if there is a stored session for this site, or a KSYNC_TOKEN in
     * the environment; otherwise null, so the caller falls back to cookies or a password
     */
    public static OAuth2Client oauth2SessionOrNull(String url) {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        OAuth2Client oauth = newOAuth2Client(url);
        return oauth.hasSession() ? oauth : null;
    }

    /**
     * The ksync url points at a branch within a repository, but the OAuth2 endpoints live at the
     * root of the website, so strip the path off.
     */
    static String baseUrlOf(String url) {
        try {
            java.net.URL u = new java.net.URL(url);
            String base = u.getProtocol() + "://" + u.getHost();
            if (u.getPort() > 0) {
                base += ":" + u.getPort();
            }
            return base;
        } catch (java.net.MalformedURLException ex) {
            throw new RuntimeException("Not a valid url: " + url, ex);
        }
    }

    /**
     * The cookie login for a site, as milton cookie names, or an empty map if there is none.
     * Reads the per-user credential store, not the checkout.
     */
    public static Map<String, String> getCookies(String url) {
        Map<String, String> map = new HashMap<>();
        if (StringUtils.isBlank(url)) {
            return map;
        }
        try {
            CredentialStore.Credentials creds = CredentialStore.defaultStore().get(baseUrlOf(url));
            if (creds != null) {
                if (StringUtils.isNotBlank(creds.userUrl)) {
                    map.put("miltonUserUrl", creds.userUrl);
                }
                if (StringUtils.isNotBlank(creds.userUrlHash)) {
                    map.put("miltonUserUrlHash", creds.userUrlHash);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not read stored credentials", ex);
        }
        return map;
    }

    /**
     * Moves a cookie login left behind by an older ksync out of the checkout and into the
     * per-user store, then strips it from ksync.properties so the secret stops sitting
     * somewhere a git commit can reach.
     *
     * Existing credentials for the host win, so re-running this cannot clobber a fresher login.
     */
    public static void migrateLegacyCredentials(String url, File repoDir) {
        Properties props = readProps(repoDir);
        String userUrl = props.getProperty("userUrl");
        String userUrlHash = props.getProperty("userUrlHash");
        if (StringUtils.isBlank(userUrl) && StringUtils.isBlank(userUrlHash)) {
            return;
        }
        if (StringUtils.isBlank(url)) {
            // without a url there is no host to key them by, so leave them be rather than
            // dropping a working login on the floor
            log.warn("Found a login in {} but no url to associate it with, leaving it in place", repoDir);
            return;
        }
        try {
            CredentialStore store = CredentialStore.defaultStore();
            String host = baseUrlOf(url);
            CredentialStore.Credentials creds = store.getOrCreate(host);
            if (StringUtils.isBlank(creds.userUrl) && StringUtils.isBlank(creds.userUrlHash)) {
                creds.userUrl = userUrl;
                creds.userUrlHash = userUrlHash;
                store.put(host, creds);
                log.info("Your saved login for {} now lives in {}, instead of inside this checkout", host, store.getPath());
            } else {
                log.debug("Discarding the login in {}, the credential store already has one for {}", repoDir, host);
            }
            props.remove("userUrl");
            props.remove("userUrlHash");
            writeProps(props, repoDir);
        } catch (IOException ex) {
            throw new RuntimeException("Could not move the saved login into the credential store", ex);
        }
    }

    @FunctionalInterface
    public interface KSyncCommand {

        void accept(File configDir, KSync3 k) throws Exception;
    }

    public static void writeProps(String url, String user, File repoDir) {
        Properties props = readProps(repoDir);
        if (StringUtils.isNotBlank(url)) {
            String oldUrl = props.getProperty("url");
            if (oldUrl != null && !oldUrl.equals(url)) {
                props.remove("remoteHash"); // no longer valid if url is changing
            }
            props.put("url", url);
        }
        if (StringUtils.isNotBlank(user)) {
            String oldUser = props.getProperty("user");
            if (oldUser != null && !oldUser.equals(user)) {
                // the saved login belongs to the previous user, so it is no longer valid
                String forUrl = StringUtils.isNotBlank(url) ? url : props.getProperty("url");
                if (StringUtils.isNotBlank(forUrl)) {
                    clearLogin(forUrl);
                }
            }
            props.put("user", user);
        }
        writeProps(props, repoDir);
    }

    public static void writeProps(Properties props, File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        log.debug("writeProps: updating file {}", file.getAbsolutePath());
        try (FileOutputStream fout = new FileOutputStream(file)) {
            props.store(fout, null);
        } catch (Throwable e) {
            log.error("Could not write the properties file {}", file.getAbsolutePath(), e);
        }
    }

    /** Records a cookie login for a site in the per-user credential store. */
    public static void writeLoginProps(String userUrl, String userUrlHash, String url) {
        try {
            CredentialStore store = CredentialStore.defaultStore();
            String host = baseUrlOf(url);
            CredentialStore.Credentials creds = store.getOrCreate(host);
            creds.userUrl = userUrl;
            creds.userUrlHash = userUrlHash;
            store.put(host, creds);
            log.info("Saved login for {} to {}", host, store.getPath());
        } catch (IOException ex) {
            throw new RuntimeException("Could not save the login", ex);
        }
    }

    /** Forgets the cookie login for a site, without touching any OAuth2 session. */
    public static void clearLogin(String url) {
        try {
            CredentialStore store = CredentialStore.defaultStore();
            String host = baseUrlOf(url);
            CredentialStore.Credentials creds = store.get(host);
            if (creds != null && (creds.userUrl != null || creds.userUrlHash != null)) {
                creds.userUrl = null;
                creds.userUrlHash = null;
                store.put(host, creds);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not clear the saved login", ex);
        }
    }

    public static Properties readProps(File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        Properties props = new Properties();
        if (file.exists()) {
            //log.info("Reading properties from {}", file.getAbsolutePath());
            try (FileInputStream fi = new FileInputStream(file)) {
                props.load(fi);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            log.debug("Properties file doesnt exist: {}", file.getAbsolutePath());
        }
        return props;
    }

    public static void saveRemoteHash(File repoDir, String rootHash) {
        Properties props = readProps(repoDir);
        props.setProperty("remoteHash", rootHash);
        writeProps(props, repoDir);
    }

    public static String getLastRemoteHash(File repoDir) {
        Properties props = readProps(repoDir);
        String s = props.getProperty("remoteHash");
        if (StringUtils.isBlank(s) || s.equals("null")) {
            return null;
        }
        return s;
    }

    public static void processHashes(JSONArray arr, Consumer<String> c) {
        for (Object o : arr) {
            String hash = o.toString();
            c.accept(hash);
        }
    }

}
