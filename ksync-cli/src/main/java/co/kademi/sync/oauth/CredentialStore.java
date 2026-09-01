package co.kademi.sync.oauth;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials for one or more Kademi hosts, in a single per-user file.
 *
 * The file lives under the user's config directory, never in a checkout: tokens belong to a
 * person, not a project, and keeping a secret in .ksync/ksync.properties puts it somewhere a
 * git commit can find it.
 *
 * The path and the JSON shape deliberately match ksync-go's auth.Store, so the two clients
 * share one login: ~/.config/ksync/credentials.json on Linux, with the platform equivalents
 * elsewhere.
 *
 * ponytail: a 0600 file, not the OS keyring, the same call ksync-go made. A keyring protects
 * against other users on the machine and against offline disk theft; 0600 covers the first and
 * full disk encryption covers the second, and reaching for a keyring means shelling out to
 * secret-tool, security or PowerShell. Revisit if credentials ever need to live on a shared box.
 */
public class CredentialStore {

    private static final Logger log = LoggerFactory.getLogger(CredentialStore.class);

    private final Path path;

    public CredentialStore(Path path) {
        this.path = path;
    }

    /**
     * Overrides where credentials are kept. Set by tests so they cannot touch the real store,
     * and usable to relocate the file.
     */
    public static final String PATH_PROPERTY = "ksync.credentialsFile";

    /** The conventional per-user location, matching ksync-go's DefaultStore. */
    public static CredentialStore defaultStore() {
        return new CredentialStore(defaultPath());
    }

    static Path defaultPath() {
        String override = System.getProperty(PATH_PROPERTY);
        if (StringUtils.isNotBlank(override)) {
            return Paths.get(override);
        }
        return userConfigDir().resolve("ksync").resolve("credentials.json");
    }

    public Path getPath() {
        return path;
    }

    /**
     * @return the credentials for a host, or null if there are none
     */
    public synchronized Credentials get(String host) throws IOException {
        return load().get(normalizeHost(host));
    }

    /** Records the credentials for a host, replacing anything already there. */
    public synchronized void put(String host, Credentials creds) throws IOException {
        Map<String, Credentials> all = load();
        if (creds == null || creds.isEmpty()) {
            all.remove(normalizeHost(host));
        } else {
            all.put(normalizeHost(host), creds);
        }
        save(all);
    }

    /**
     * Reads the credentials for a host, or returns a blank set to fill in. Never null, so
     * callers can update a field and put it straight back.
     */
    public synchronized Credentials getOrCreate(String host) throws IOException {
        Credentials c = get(host);
        return c == null ? new Credentials() : c;
    }

    /** Forgets a host, which is what a logout does. */
    public synchronized void delete(String host) throws IOException {
        Map<String, Credentials> all = load();
        if (all.remove(normalizeHost(host)) != null) {
            save(all);
        }
    }

    /** The hosts with stored credentials, sorted. */
    public synchronized Set<String> hosts() throws IOException {
        return new TreeSet<>(load().keySet());
    }

    private Map<String, Credentials> load() throws IOException {
        if (!Files.exists(path)) {
            return new HashMap<>();
        }
        String text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        if (StringUtils.isBlank(text)) {
            return new HashMap<>();
        }
        JSONObject json;
        try {
            json = JSONObject.fromObject(text);
        } catch (RuntimeException ex) {
            // Unlike a cache this cannot be quietly discarded and rebuilt: throwing it away
            // would log the user out and lose their refresh token.
            throw new IOException("Credentials file " + path + " is not valid JSON, refusing to overwrite it", ex);
        }
        Map<String, Credentials> all = new HashMap<>();
        for (Object key : json.keySet()) {
            all.put(String.valueOf(key), Credentials.fromJson(json.getJSONObject(String.valueOf(key))));
        }
        return all;
    }

    private void save(Map<String, Credentials> all) throws IOException {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, Credentials> entry : all.entrySet()) {
            json.put(entry.getKey(), entry.getValue().toJson());
        }
        byte[] data = (json.toString(2) + "\n").getBytes(StandardCharsets.UTF_8);

        Path dir = path.getParent();
        Files.createDirectories(dir);
        restrictToOwner(dir, true);

        // Write beside the target and rename in, so a crash leaves the previous contents
        // rather than a truncated file and no reader sees a half written one.
        Path tmp = Files.createTempFile(dir, path.getFileName().toString() + "-", ".tmp");
        try {
            restrictToOwner(tmp, false);
            Files.write(tmp, data);
            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
        restrictToOwner(path, false);
    }

    /**
     * A token is readable by anything that can read the file, so the mode is the whole
     * protection. Falls back to the File api where posix permissions are not supported.
     */
    private static void restrictToOwner(Path target, boolean directory) {
        try {
            Files.setPosixFilePermissions(target, PosixFilePermissions.fromString(directory ? "rwx------" : "rw-------"));
            return;
        } catch (UnsupportedOperationException | IOException ex) {
            log.debug("Could not set posix permissions on {}, falling back", target, ex);
        }
        File f = target.toFile();
        f.setReadable(false, false);
        f.setWritable(false, false);
        f.setReadable(true, true);
        f.setWritable(true, true);
        if (directory) {
            f.setExecutable(false, false);
            f.setExecutable(true, true);
        }
    }

    /** Mirrors Go's os.UserConfigDir, so both clients land on the same file. */
    static Path userConfigDir() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            String appData = System.getenv("AppData");
            if (StringUtils.isNotBlank(appData)) {
                return Paths.get(appData);
            }
        } else if (os.contains("mac") || os.contains("darwin")) {
            return Paths.get(System.getProperty("user.home"), "Library", "Application Support");
        }
        String xdg = System.getenv("XDG_CONFIG_HOME");
        if (StringUtils.isNotBlank(xdg)) {
            return Paths.get(xdg);
        }
        return Paths.get(System.getProperty("user.home"), ".config");
    }

    /**
     * Credentials are keyed by host so one file serves several Kademi accounts, and a url with
     * or without a scheme, path or trailing slash finds the same entry. The port is part of the
     * key, because a dev instance on another port is a different site.
     */
    static String normalizeHost(String host) {
        String s = StringUtils.trimToEmpty(host).toLowerCase();
        s = StringUtils.removeStart(s, "https://");
        s = StringUtils.removeStart(s, "http://");
        int slash = s.indexOf('/');
        return slash >= 0 ? s.substring(0, slash) : s;
    }

    /**
     * What a successful login leaves behind for one host. Field names match ksync-go's
     * auth.Credentials so the file is readable by both.
     */
    public static class Credentials {

        /**
         * This install's own dynamically registered client. Each install registers separately,
         * so two machines never share a client and cannot invalidate each other's tokens.
         */
        public String clientId;
        /**
         * Normally empty: a CLI registers as a public client, which makes KOAuth2 enforce PKCE.
         * Here only for a site which insists on a confidential client.
         */
        public String clientSecret;
        public String accessToken;
        public String refreshToken;
        /** Epoch millis when the access token stops working. 0 means unknown. */
        public long expiresAt;
        public List<String> scopes = new ArrayList<>();
        /** Recorded so a refresh costs no discovery round trip. */
        public String tokenEndpoint;
        /** The redirect uri bound to the registration, so the listener reclaims its port. */
        public String redirectUri;
        /**
         * The older cookie login: the miltonUserUrl and miltonUserUrlHash values ksync scrapes
         * off a login response. Kept here rather than in the checkout for the same reason as the
         * tokens. Not written by ksync-go, which ignores fields it does not know.
         */
        public String userUrl;
        public String userUrlHash;

        /** @return true if there is nothing left worth storing for this host */
        boolean isEmpty() {
            return StringUtils.isBlank(clientId) && StringUtils.isBlank(accessToken)
                    && StringUtils.isBlank(refreshToken) && StringUtils.isBlank(userUrl)
                    && StringUtils.isBlank(userUrlHash);
        }

        static Credentials fromJson(JSONObject json) {
            Credentials c = new Credentials();
            c.clientId = optString(json, "client_id");
            c.clientSecret = optString(json, "client_secret");
            c.accessToken = optString(json, "access_token");
            c.refreshToken = optString(json, "refresh_token");
            c.tokenEndpoint = optString(json, "token_endpoint");
            c.redirectUri = optString(json, "redirect_uri");
            c.userUrl = optString(json, "user_url");
            c.userUrlHash = optString(json, "user_url_hash");
            String expires = optString(json, "expires_at");
            if (StringUtils.isNotBlank(expires)) {
                try {
                    // ksync-go writes RFC3339 with a numeric offset, eg 2026-08-29T13:20:45+12:00.
                    // Instant.parse rejects that on java 8 and 11 (verified; it works on 17+),
                    // and this is built for 8. OffsetDateTime takes both that and a trailing Z
                    // on every version, so it is the portable choice. Do not "simplify" this to
                    // Instant.parse: readsAnRfc3339ExpiryWithANumericOffset covers it.
                    c.expiresAt = java.time.OffsetDateTime.parse(expires).toInstant().toEpochMilli();
                } catch (RuntimeException ex) {
                    log.warn("Could not read expires_at '{}', treating the token as expired", expires);
                }
            }
            if (json.containsKey("scopes") && !json.get("scopes").equals(null)) {
                JSONArray arr = json.getJSONArray("scopes");
                for (int i = 0; i < arr.size(); i++) {
                    c.scopes.add(arr.getString(i));
                }
            }
            return c;
        }

        JSONObject toJson() {
            JSONObject json = new JSONObject();
            json.put("client_id", StringUtils.defaultString(clientId));
            if (StringUtils.isNotBlank(clientSecret)) {
                json.put("client_secret", clientSecret);
            }
            json.put("access_token", StringUtils.defaultString(accessToken));
            if (StringUtils.isNotBlank(refreshToken)) {
                json.put("refresh_token", refreshToken);
            }
            if (expiresAt > 0) {
                json.put("expires_at", java.time.Instant.ofEpochMilli(expiresAt).toString());
            }
            if (scopes != null && !scopes.isEmpty()) {
                json.put("scopes", JSONArray.fromObject(scopes));
            }
            if (StringUtils.isNotBlank(tokenEndpoint)) {
                json.put("token_endpoint", tokenEndpoint);
            }
            if (StringUtils.isNotBlank(redirectUri)) {
                json.put("redirect_uri", redirectUri);
            }
            if (StringUtils.isNotBlank(userUrl)) {
                json.put("user_url", userUrl);
            }
            if (StringUtils.isNotBlank(userUrlHash)) {
                json.put("user_url_hash", userUrlHash);
            }
            return json;
        }

        /**
         * ksync-go writes an empty access_token rather than omitting it, so blank and absent
         * have to mean the same thing here or a logged out entry looks like a session.
         */
        private static String optString(JSONObject json, String key) {
            if (!json.containsKey(key) || json.get(key).equals(null)) {
                return null;
            }
            return StringUtils.trimToNull(json.getString(key));
        }
    }
}
