package co.kademi.sync;

import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The version state of a repository, as reported by its repo-meta.json: which version is live,
 * which is the latest, and every version with its content hash.
 *
 * Lets a checkout be pointed at a repository rather than at one frozen version. Which version is
 * latest is decided by the server, which compares version numbers component by component, so
 * 1.2.15 is later than 1.2.9 and a branch called something that is not a version number is never
 * chosen. This deliberately does not re-derive that from the versions list: two implementations of
 * "latest" would eventually disagree, and the disagreement would be silent.
 */
public class RepoMeta {

    private static final Logger log = LoggerFactory.getLogger(RepoMeta.class);

    public static final String META_NAME = "repo-meta.json";

    /** Whether, and how hard, to ask the server which version this checkout should be on. */
    public enum Tracking {
        /** The url names a version. Nothing to ask, and no request is made. */
        OFF,
        /** The url is new, so it might name either a repository or a version - find out. */
        PROBE,
        /** The url is known to name a repository, so its latest version has to be resolved. */
        TRACK
    }

    /**
     * Reads the repository metadata, if this url names a repository.
     *
     * @param client the host to ask, already carrying the login
     * @param repoUrl the url to test, with or without a trailing slash
     * @return the metadata, or null if this url does not name a repository - which is the normal
     * answer for a url naming a version, and for a server without the endpoint
     */
    public static RepoMeta fetch(Host client, String repoUrl) throws IOException {
        String path = metaPath(repoUrl);
        byte[] resp;
        try {
            resp = client.get(path);
        } catch (NotFoundException ex) {
            log.debug("No {} at {}, so this url does not name a repository", META_NAME, path);
            return null;
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException ex) {
            throw new IOException("Could not read " + path + ": " + ex.getMessage(), ex);
        }
        if (resp == null) {
            return null;
        }
        String body = new String(resp, StandardCharsets.UTF_8).trim();
        if (!body.startsWith("{")) {
            // A version can hold a file of this name, and an older server may answer with a page
            log.debug("{} did not return JSON, so this url does not name a repository", path);
            return null;
        }
        RepoMeta meta = parse(body);
        return meta.isRepository() ? meta : null;
    }

    /**
     * @param json the body of a repo-meta.json
     * @return the metadata, with a null versions list if this is not a repo-meta document
     * @throws SetupException if it is not readable as JSON at all
     */
    public static RepoMeta parse(String json) {
        JSONObject ob;
        try {
            ob = JSONObject.fromObject(json);
        } catch (JSONException ex) {
            throw new SetupException("Could not read the repository metadata: " + ex.getMessage(), ex);
        }
        JSONArray arr = ob.optJSONArray("versions");
        List<Version> versions = null;
        if (arr != null) {
            versions = new ArrayList<>();
            for (Object o : arr) {
                versions.add(toVersion((JSONObject) o));
            }
        }
        return new RepoMeta(str(ob, "name"), toVersion(ob.optJSONObject("liveVersion")),
                toVersion(ob.optJSONObject("latestVersion")), versions);
    }

    /** The path of the metadata document for a repository url. */
    static String metaPath(String repoUrl) {
        String path = withTrailingSlash(repoUrl);
        int schemeAt = path.indexOf("://");
        if (schemeAt >= 0) {
            int pathAt = path.indexOf('/', schemeAt + 3);
            path = pathAt < 0 ? "/" : path.substring(pathAt);
        }
        return path + META_NAME;
    }

    /** The url of one version within a repository. */
    public static String versionUrl(String repoUrl, String versionName) {
        return withTrailingSlash(repoUrl) + versionName + "/";
    }

    /**
     * The version name a checkout url ends in, for reporting a move from one version to another.
     *
     * @return the last path segment, or the whole url if it has no path
     */
    public static String versionNameOf(String versionUrl) {
        String s = StringUtils.stripEnd(versionUrl, "/");
        int slash = s.lastIndexOf('/');
        return slash < 0 ? versionUrl : s.substring(slash + 1);
    }

    /** A url ending in a slash, so one can be tested as a prefix of another without matching half a name. */
    static String withTrailingSlash(String url) {
        String s = url.trim();
        return s.endsWith("/") ? s : s + "/";
    }

    private static Version toVersion(JSONObject ob) {
        if (ob == null) {
            return null;
        }
        return new Version(str(ob, "name"), str(ob, "hash"), ob.optBoolean("hidden"), ob.optBoolean("readonly"));
    }

    private static String str(JSONObject ob, String field) {
        Object o = ob.get(field);
        if (o == null || o instanceof JSONNull) {
            return null;
        }
        String s = String.valueOf(o);
        return StringUtils.isBlank(s) ? null : s;
    }

    private final String name;
    private final Version liveVersion;
    private final Version latestVersion;
    private final List<Version> versions;

    public RepoMeta(String name, Version liveVersion, Version latestVersion, List<Version> versions) {
        this.name = name;
        this.liveVersion = liveVersion;
        this.latestVersion = latestVersion;
        this.versions = versions;
    }

    public String getName() {
        return name;
    }

    /**
     * Whether this document is repository metadata at all. A version can hold a file called
     * repo-meta.json of its own, and some other JSON is not an answer to the question being asked.
     *
     * @return true if the document has a versions list
     */
    public boolean isRepository() {
        return versions != null;
    }

    /** @return the version the website is serving, or null if none is live */
    public Version getLiveVersion() {
        return liveVersion;
    }

    /**
     * @return the highest version number on this repository, or null when no version has a version
     * number for a name - a repository holding only hand-named branches has no latest
     */
    public Version getLatestVersion() {
        return latestVersion;
    }

    public List<Version> getVersions() {
        return versions == null ? new ArrayList<>() : versions;
    }

    /** The version names, for telling someone what they could have picked. */
    public List<String> versionNames() {
        List<String> names = new ArrayList<>();
        for (Version v : getVersions()) {
            names.add(v.getName());
        }
        return names;
    }

    public static class Version {

        private final String name;
        private final String hash;
        private final boolean hidden;
        private final boolean readonly;

        public Version(String name, String hash, boolean hidden, boolean readonly) {
            this.name = name;
            this.hash = hash;
            this.hidden = hidden;
            this.readonly = readonly;
        }

        public String getName() {
            return name;
        }

        /** @return the content hash of this version's head commit, or null if it has no content */
        public String getHash() {
            return hash;
        }

        public boolean isHidden() {
            return hidden;
        }

        public boolean isReadonly() {
            return readonly;
        }
    }
}
