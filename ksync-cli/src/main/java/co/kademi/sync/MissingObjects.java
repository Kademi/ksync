package co.kademi.sync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * The objects a version needs which are not in the server's stores, as reported by the
 * findMissingObjects check on the branch.
 *
 * The question being asked is which *files* are broken, not which hashes are absent, so everything
 * here is organised by the file that needs the object. The server reports both - a flat list per
 * object type, which is what the push path consumes, and the same objects attributed to a path,
 * which is what a person reading the output needs.
 *
 * Parsing and formatting are kept apart from the request so they can be tested without a server.
 */
public class MissingObjects {

    // As sent by VfsMissingData on the server
    public static final String TYPE_DIRECTORY = "directory";
    public static final String TYPE_FILE_FANOUT = "fileFanout";
    public static final String TYPE_CHUNK_FANOUT = "chunkFanout";
    public static final String TYPE_BLOB = "blob";

    /** Hashes listed for one file before the rest are counted instead. */
    private static final int MAX_PER_FILE = 5;

    /**
     * @param response the body of the findMissingObjects POST
     * @return what the server says is missing, empty when nothing is
     * @throws SetupException if the server did not answer with a result this can read
     */
    public static MissingObjects parse(String response) {
        if (StringUtils.isBlank(response)) {
            throw new SetupException("The server returned an empty response to the missing object check");
        }
        String s = response.trim();
        if (!s.startsWith("{")) {
            // An older server does not know the findMissingObjects parameter, falls through to
            // rendering the branch page, and answers with HTML. Say that, rather than letting a
            // JSON parser complain about a '<'.
            throw new SetupException("The server did not answer the missing object check with JSON."
                    + " It is probably an older server, without kademi-dev issue 24539."
                    + " The response began: " + StringUtils.abbreviate(s.replaceAll("\\s+", " "), 120));
        }
        JSONObject json;
        try {
            json = JSONObject.fromObject(s);
        } catch (JSONException ex) {
            throw new SetupException("Could not read the server's response to the missing object check: " + ex.getMessage(), ex);
        }
        if (!json.optBoolean("status")) {
            throw new SetupException("The server could not check this version: " + messageOf(json));
        }
        return new MissingObjects(objectsOf(json.optJSONObject("data")));
    }

    private static String messageOf(JSONObject json) {
        JSONArray messages = json.optJSONArray("messages");
        if (messages == null || messages.isEmpty()) {
            return "no reason given";
        }
        List<String> list = new ArrayList<>();
        for (Object o : messages) {
            list.add(String.valueOf(o));
        }
        return StringUtils.join(list, ". ");
    }

    private static List<MissingObject> objectsOf(JSONObject data) {
        List<MissingObject> list = new ArrayList<>();
        if (data == null) {
            return list;
        }
        JSONArray arr = data.optJSONArray("objects");
        if (arr != null && !arr.isEmpty()) {
            for (Object o : arr) {
                JSONObject ob = (JSONObject) o;
                list.add(new MissingObject(str(ob, "type"), str(ob, "path"), str(ob, "hash")));
            }
            return list;
        }
        // No attributed objects, but the flat lists may still say something is wrong. Reporting
        // those without a path is far better than reporting a broken version as clean.
        addFlat(list, data, "missingDirectories", TYPE_DIRECTORY);
        addFlat(list, data, "missingFileFanouts", TYPE_FILE_FANOUT);
        addFlat(list, data, "missingChunkFanouts", TYPE_CHUNK_FANOUT);
        addFlat(list, data, "missingBlobs", TYPE_BLOB);
        return list;
    }

    private static void addFlat(List<MissingObject> list, JSONObject data, String field, String type) {
        JSONArray arr = data.optJSONArray(field);
        if (arr == null) {
            return;
        }
        for (Object o : arr) {
            list.add(new MissingObject(type, null, String.valueOf(o)));
        }
    }

    private static String str(JSONObject ob, String field) {
        Object o = ob.get(field);
        if (o == null || o instanceof JSONNull) {
            return null;
        }
        String s = String.valueOf(o);
        return StringUtils.isBlank(s) ? null : s;
    }

    private final List<MissingObject> objects;

    public MissingObjects(List<MissingObject> objects) {
        this.objects = objects == null ? new ArrayList<>() : objects;
    }

    public List<MissingObject> getObjects() {
        return objects;
    }

    public boolean isEmpty() {
        return objects.isEmpty();
    }

    /** @return the number of objects of a type, one of the TYPE_ constants */
    public int count(String type) {
        int count = 0;
        for (MissingObject o : objects) {
            if (type.equals(o.getType())) {
                count++;
            }
        }
        return count;
    }

    /**
     * The report as it appears in the CLI output, one line per element, ready to log.
     *
     * @return the lines, never empty
     */
    public List<String> report() {
        List<String> lines = new ArrayList<>();
        if (objects.isEmpty()) {
            lines.add("Nothing missing. Every object this version needs is on the server");
            return lines;
        }

        // Directories first, and on their own. A missing listing is a different kind of fault: the
        // walk cannot see past it, so everything below it is unchecked rather than known to be fine.
        List<MissingObject> dirs = new ArrayList<>();
        Map<String, List<MissingObject>> files = new LinkedHashMap<>();
        for (MissingObject o : objects) {
            if (TYPE_DIRECTORY.equals(o.getType())) {
                dirs.add(o);
            } else {
                String path = o.getPath() == null ? "(the server did not say which file)" : o.getPath();
                files.computeIfAbsent(path, p -> new ArrayList<>()).add(o);
            }
        }

        if (!dirs.isEmpty()) {
            lines.add(plural(dirs.size(), "directory listing is", "directory listings are")
                    + " missing, so nothing below could be checked:");
            lines.add("");
            for (MissingObject o : dirs) {
                lines.add("  " + (o.getPath() == null ? "(the server did not say which directory)" : o.getPath()));
                lines.add("      " + StringUtils.rightPad(o.getType(), 12) + " " + o.getHash());
            }
            lines.add("");
        }

        if (!files.isEmpty()) {
            lines.add(plural(files.size(), "file is", "files are") + " missing objects:");
            lines.add("");
            for (Map.Entry<String, List<MissingObject>> entry : files.entrySet()) {
                lines.add("  " + entry.getKey());
                List<MissingObject> forPath = entry.getValue();
                for (MissingObject o : forPath.subList(0, Math.min(MAX_PER_FILE, forPath.size()))) {
                    lines.add("      " + StringUtils.rightPad(o.getType(), 12) + " " + o.getHash());
                }
                if (forPath.size() > MAX_PER_FILE) {
                    // A file split into hundreds of chunks would otherwise bury every other file
                    List<MissingObject> rest = forPath.subList(MAX_PER_FILE, forPath.size());
                    lines.add("      (and " + plural(rest.size(), "more " + nameOf(typeOf(rest), rest.size())) + ")");
                }
            }
            lines.add("");
        }

        lines.add("Missing: " + StringUtils.join(new String[]{
            plural(count(TYPE_BLOB), nameOf(TYPE_BLOB, count(TYPE_BLOB))),
            plural(count(TYPE_CHUNK_FANOUT), nameOf(TYPE_CHUNK_FANOUT, count(TYPE_CHUNK_FANOUT))),
            plural(count(TYPE_FILE_FANOUT), nameOf(TYPE_FILE_FANOUT, count(TYPE_FILE_FANOUT))),
            plural(count(TYPE_DIRECTORY), nameOf(TYPE_DIRECTORY, count(TYPE_DIRECTORY)))
        }, ", "));
        return lines;
    }

    /** @return the single type shared by these objects, or null if they are of more than one */
    private static String typeOf(List<MissingObject> list) {
        String type = list.get(0).getType();
        for (MissingObject o : list) {
            if (!StringUtils.equals(type, o.getType())) {
                return null;
            }
        }
        return type;
    }

    /** The type as it reads in a sentence: a plain name, pluralised for a count other than one. */
    private static String nameOf(String type, int count) {
        String name;
        if (TYPE_DIRECTORY.equals(type)) {
            return count == 1 ? "directory" : "directories";
        } else if (TYPE_FILE_FANOUT.equals(type)) {
            name = "file fanout";
        } else if (TYPE_CHUNK_FANOUT.equals(type)) {
            name = "chunk fanout";
        } else if (TYPE_BLOB.equals(type)) {
            name = "blob";
        } else {
            name = "object";
        }
        return count == 1 ? name : name + "s";
    }

    private static String plural(int count, String singular, String plural) {
        return count + " " + (count == 1 ? singular : plural);
    }

    /** For a name already pluralised by nameOf, which is where the irregular cases are handled. */
    private static String plural(int count, String name) {
        return count + " " + name;
    }

    public static class MissingObject {

        private final String type;
        private final String path;
        private final String hash;

        public MissingObject(String type, String path, String hash) {
            this.type = type;
            this.path = path;
            this.hash = hash;
        }

        /** @return one of the TYPE_ constants, saying what kind of object is missing */
        public String getType() {
            return type;
        }

        /** @return the path of the file which needs it, or null if the server did not attribute it */
        public String getPath() {
            return path;
        }

        public String getHash() {
            return hash;
        }
    }
}
