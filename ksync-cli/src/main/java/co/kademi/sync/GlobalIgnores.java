package co.kademi.sync;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ignore patterns that apply to every checkout for this user, kept in a single file in the
 * home directory.
 *
 * The per-checkout -ignore option only helps someone who remembers to pass it in every project.
 * The things people actually want gone are a property of the machine, not of any one repository,
 * so they belong in one file the user edits once.
 *
 * The worst offenders do not even belong there: see {@link #BUILT_IN}.
 *
 * The format is gitignore's, minus the parts that need a path: one pattern per line, blank lines
 * and lines starting with # skipped. Patterns match a file or folder name anywhere in the tree,
 * because that is the only thing the ignore checks downstream are given.
 */
public class GlobalIgnores {

    private static final Logger log = LoggerFactory.getLogger(GlobalIgnores.class);

    /**
     * Overrides where the ignore file is kept. Set by tests so they cannot touch the real one,
     * and usable to relocate the file.
     */
    public static final String PATH_PROPERTY = "ksync.ignoreFile";

    /**
     * Patterns that are always ignored, whatever the ignore file says.
     *
     * node_modules is why this exists. Leaving it to configuration means every developer has to
     * know to add it before their first sync, and the way they find out is by syncing a hundred
     * thousand files they did not mean to and then working out why. A default that only protects
     * the people who already knew about it is not much of a default.
     *
     * Deliberately short. Anything in here cannot be turned off, so it is limited to names that
     * are generated rather than written, and would never be deployed. Notably absent are dist,
     * build and target: those are build output for most projects but a legitimate part of an app
     * for some, and silently refusing to deploy one would be far harder to diagnose than a large
     * first sync. They belong in the ignore file, per machine, where they can be undone.
     *
     * Dot names need no entry here - anything starting with a dot is already skipped, along with
     * .ksync itself, by {@link io.milton.sync.Utils#ignored(java.io.File, List)}.
     */
    public static final List<String> BUILT_IN = Collections.unmodifiableList(Arrays.asList(
            "node_modules",
            "bower_components",
            "Thumbs.db"));

    private final Path path;

    public GlobalIgnores(Path path) {
        this.path = path;
    }

    /** The conventional per-user location, ~/.ksyncignore */
    public static GlobalIgnores defaultIgnores() {
        return new GlobalIgnores(defaultPath());
    }

    static Path defaultPath() {
        String override = System.getProperty(PATH_PROPERTY);
        if (StringUtils.isNotBlank(override)) {
            return Paths.get(override);
        }
        return Paths.get(System.getProperty("user.home"), ".ksyncignore");
    }

    public Path getPath() {
        return path;
    }

    public boolean exists() {
        return Files.exists(path);
    }

    /**
     * @return the patterns in the file, in the order they appear, or an empty list if there is
     * no file yet. Never null, and never throws: a broken ignore file must not stop a sync.
     */
    public List<String> patterns() {
        if (!Files.exists(path)) {
            log.debug("No global ignore file at {}", path);
            return Collections.emptyList();
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            log.warn("Could not read the global ignore file {}, continuing without it", path, ex);
            return Collections.emptyList();
        }
        List<String> patterns = new ArrayList<>();
        for (String line : lines) {
            String s = clean(line);
            if (s != null) {
                patterns.add(s);
            }
        }
        return patterns;
    }

    /**
     * Whether a pattern is one of the built in ones, and so already in force. Cleaned the same
     * way the file is read, so "node_modules/" is recognised as "node_modules".
     */
    public static boolean isBuiltIn(String pattern) {
        String s = clean(pattern);
        return s != null && BUILT_IN.contains(s);
    }

    /**
     * Adds a pattern, creating the file if this is the first one.
     *
     * @return true if it was added, false if the file already had it
     */
    public boolean add(String pattern) throws IOException {
        String s = clean(pattern);
        if (s == null) {
            throw new IllegalArgumentException("Not a usable ignore pattern: " + pattern);
        }
        if (BUILT_IN.contains(s) || patterns().contains(s)) {
            return false;
        }
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        // Append rather than rewrite, so any comments or ordering the user put in the file survive.
        // A file that does not end in a newline would otherwise glue the new pattern onto the last one.
        StringBuilder sb = new StringBuilder();
        if (Files.exists(path) && needsLeadingNewline()) {
            sb.append(System.lineSeparator());
        }
        sb.append(s).append(System.lineSeparator());
        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return true;
    }

    private boolean needsLeadingNewline() throws IOException {
        long size = Files.size(path);
        if (size == 0) {
            return false;
        }
        byte[] all = Files.readAllBytes(path);
        byte last = all[all.length - 1];
        return last != '\n' && last != '\r';
    }

    /**
     * @return the pattern with comments, blank lines and a trailing slash removed, or null if
     * the line carries no pattern. A trailing slash is how gitignore says "directory only", and
     * the checks downstream match names without knowing which they have, so drop it.
     */
    private static String clean(String line) {
        if (line == null) {
            return null;
        }
        String s = line.trim();
        if (s.isEmpty() || s.startsWith("#")) {
            return null;
        }
        while (s.endsWith("/") && s.length() > 1) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s.isEmpty() ? null : s;
    }

    /**
     * The built in patterns, the global ones and the ones given for this run, without duplicates.
     *
     * This is the one place an ignore list is built for a run - the sync commands go through
     * KSyncUtils and publish goes through AppDeployer, and both call this - so the built ins
     * being added here is what makes them unavoidable.
     *
     * @param localIgnores patterns from -ignore or ksync.properties, may be null
     */
    public static List<String> combine(List<String> localIgnores) {
        return combine(defaultIgnores(), localIgnores);
    }

    static List<String> combine(GlobalIgnores global, List<String> localIgnores) {
        // built ins first, so a list read back reads in order of how hard it is to change
        Set<String> all = new LinkedHashSet<>(BUILT_IN);
        all.addAll(global.patterns());
        if (localIgnores != null) {
            all.addAll(localIgnores);
        }
        // Never null now, where it used to be when nothing was configured anywhere. Callers pass
        // the result straight to the ignore checks, which still read null as "ignore nothing".
        return new ArrayList<>(all);
    }
}
