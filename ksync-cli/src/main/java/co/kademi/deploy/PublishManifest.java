package co.kademi.deploy;

import co.kademi.sync.SetupException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseError;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlPosition;
import org.tomlj.TomlTable;

/** Which folders publish sweeps and in what order, from the ksync.toml ksync-go reads too (its publish/manifest.go). */
final class PublishManifest {

    static final String FILE = "ksync.toml";
    static final List<String> TYPES = Arrays.asList("app", "lib", "theme", "recipe");
    private static final Set<String> TOP_KEYS = new HashSet<>(Arrays.asList("concurrency", "tiers"));
    private static final Set<String> TIER_KEYS = new HashSet<>(Arrays.asList("dir", "type", "first"));

    static final class Tier {

        final String dir;
        final String type;
        final List<String> first;

        Tier(String dir, String type, List<String> first) {
            this.dir = dir;
            this.type = type;
            this.first = first;
        }
    }

    final List<Tier> tiers;

    private PublishManifest(List<Tier> tiers) {
        this.tiers = tiers;
    }

    /** @return null without a ksync.toml in dir, which keeps publish's built in order */
    static PublishManifest load(File dir) {
        Path path = dir.toPath().resolve(FILE);
        if (!Files.exists(path)) {
            return null;
        }
        TomlParseResult toml;
        try {
            toml = Toml.parse(path);
        } catch (IOException ex) {
            throw fail("could not read it: " + ex.getMessage());
        }
        if (!toml.errors().isEmpty()) {
            TomlParseError e = toml.errors().get(0);
            throw fail("line " + e.position().line() + ": " + e.getMessage());
        }

        // Refused rather than ignored: a misspelt "first" would silently lose the order it exists for
        Map<String, Integer> unknown = new TreeMap<>();
        unknownKeys(toml, TOP_KEYS, "", unknown);
        TomlArray tierArray = toml.isArray("tiers") ? toml.getArray("tiers") : null;
        if (toml.contains("tiers") && (tierArray == null || !all(tierArray, TomlArray::isTable))) {
            throw fail("tiers must be [[tiers]] tables");
        }
        for (int i = 0; tierArray != null && i < tierArray.size(); i++) {
            unknownKeys(tierArray.getTable(i), TIER_KEYS, "tiers.", unknown);
        }
        if (!unknown.isEmpty()) {
            List<String> named = new ArrayList<>();
            unknown.forEach((key, line) -> named.add(key + " (line " + line + ")"));
            throw fail("unknown key" + (unknown.size() > 1 ? "s" : "") + ": " + String.join(", ", named));
        }

        Object concurrency = toml.get("concurrency");
        if (concurrency != null && !(concurrency instanceof Long)) {
            throw fail("concurrency must be a whole number");
        }
        if (concurrency != null && (Long) concurrency < 0) {
            throw fail("concurrency is " + concurrency + "; it must be positive");
        }
        if (tierArray == null || tierArray.isEmpty()) {
            throw fail("no tiers, so it would publish nothing");
        }

        List<Tier> tiers = new ArrayList<>();
        Map<String, Integer> seen = new HashMap<>();
        for (int i = 0; i < tierArray.size(); i++) {
            Tier tier;
            try {
                tier = tier(tierArray.getTable(i));
            } catch (IllegalArgumentException ex) {
                throw fail("tier " + (i + 1) + ": " + ex.getMessage());
            }
            Integer earlier = seen.put(tier.dir, i + 1);
            if (earlier != null) {
                throw fail("tier " + (i + 1) + ": \"" + tier.dir + "\" is already tier " + earlier);
            }
            tiers.add(tier);
        }
        return new PublishManifest(Collections.unmodifiableList(tiers));
    }

    private static Tier tier(TomlTable t) {
        String dir = string(t, "dir");
        if (dir == null || dir.isEmpty()) {
            throw new IllegalArgumentException("no dir");
        }
        // Joined to the folder the file is in, so a path could reach outside it
        if (dir.contains("/") || dir.contains("\\") || dir.equals(".") || dir.equals("..")) {
            throw new IllegalArgumentException("dir \"" + dir + "\" must be a single directory name, not a path");
        }
        String type = string(t, "type");
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("\"" + dir + "\" has no type");
        }
        if (!TYPES.contains(type)) {
            throw new IllegalArgumentException("dir \"" + dir + "\" has type \"" + type + "\"; expected one of " + String.join(", ", TYPES));
        }
        List<String> first = new ArrayList<>();
        if (t.contains("first")) {
            TomlArray names = t.isArray("first") ? t.getArray("first") : null;
            if (names == null || !all(names, TomlArray::isString)) {
                throw new IllegalArgumentException("dir \"" + dir + "\" has a first that is not a list of names");
            }
            for (int i = 0; i < names.size(); i++) {
                String name = names.getString(i);
                if (name.trim().isEmpty()) {
                    throw new IllegalArgumentException("dir \"" + dir + "\" lists a blank name in first");
                }
                if (first.contains(name)) {
                    throw new IllegalArgumentException("dir \"" + dir + "\" lists \"" + name + "\" twice in first");
                }
                first.add(name);
            }
        }
        return new Tier(dir, type, Collections.unmodifiableList(first));
    }

    private static String string(TomlTable t, String key) {
        Object v = t.get(key);
        if (v != null && !(v instanceof String)) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        return (String) v;
    }

    private static void unknownKeys(TomlTable table, Set<String> known, String prefix, Map<String, Integer> unknown) {
        for (String key : table.keySet()) {
            if (!known.contains(key)) {
                TomlPosition at = table.inputPositionOf(Collections.singletonList(key));
                unknown.put(prefix + key, at == null ? 0 : at.line());
            }
        }
    }

    /** The first names that exist, in the order listed, then the rest as given; first names that do not exist go in missing. */
    static List<String> order(List<String> first, List<String> names, List<String> missing) {
        List<String> ordered = new ArrayList<>();
        for (String name : first) {
            if (names.contains(name)) {
                ordered.add(name);
            } else {
                missing.add(name);
            }
        }
        for (String name : names) {
            if (!ordered.contains(name)) {
                ordered.add(name);
            }
        }
        return ordered;
    }

    private static boolean all(TomlArray array, java.util.function.BiPredicate<TomlArray, Integer> test) {
        for (int i = 0; i < array.size(); i++) {
            if (!test.test(array, i)) {
                return false;
            }
        }
        return true;
    }

    private static SetupException fail(String message) {
        return new SetupException(FILE + ": " + message);
    }
}
