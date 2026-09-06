package io.milton.sync;

import io.milton.common.Path;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.PatternSyntaxException;
import org.hashsplit4j.triplets.ITriplet;

/**
 *
 * @author brad
 */
public class Utils {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Utils.class);

    /** Stands in for a pattern that would not compile as a glob, so it is only reported once. */
    private static final PathMatcher NO_MATCH = (java.nio.file.Path p) -> false;
    private static final Map<String, PathMatcher> GLOBS = new ConcurrentHashMap<>();

    public static boolean ignored(File childFile) {
        return ignored(childFile, null);
    }

    public static boolean ignored(File childFile, List<String> ignoredPatterns) {
        if( childFile.getName().equals(".ksync") ) { // usually ignore resources starting with a dot, but special case for .mil directory
            return true;
        }
        if( matchesAny(childFile.getName(), ignoredPatterns) ) {
            return true;
        }
        return childFile.isHidden() || childFile.getName().startsWith(".");
    }

    /**
     * Whether a file or folder name matches any of the given ignore patterns.
     *
     * @param patterns may be null or empty, meaning nothing is ignored
     */
    public static boolean matchesAny(String name, List<String> patterns) {
        if (name == null || patterns == null) {
            return false;
        }
        for (String pattern : patterns) {
            if (matches(name, pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches a name against one ignore pattern.
     *
     * Patterns are globs, because that is what everyone expects from an ignore file and what
     * they will write: *.log, node_modules, build-?. Passing a glob straight to String.matches
     * used to throw, since a leading * is not a legal regex.
     *
     * A pattern that is not a glob match is then tried as a regex, so the patterns people
     * already have in -ignore and ksync.properties keep working. An invalid regex just does not
     * match, rather than killing the sync.
     */
    public static boolean matches(String name, String pattern) {
        if (name == null || pattern == null || pattern.isEmpty()) {
            return false;
        }
        if (name.equals(pattern)) {
            return true;
        }
        PathMatcher glob = globMatcher(pattern);
        if (glob != null) {
            try {
                if (glob.matches(Paths.get(name))) {
                    return true;
                }
            } catch (InvalidPathException ex) {
                // a name this platform will not accept as a path cannot match a glob, but it can
                // still match a regex, so fall through rather than returning
                log.debug("Not a usable path name, skipping the glob check: {}", name);
            }
        }
        try {
            return name.matches(pattern);
        } catch (PatternSyntaxException ex) {
            log.debug("Ignore pattern is neither a glob nor a regex, skipping it: {}", pattern);
            return false;
        }
    }

    /** Compiled globs are cached because the ignore checks run once per file on every scan. */
    private static PathMatcher globMatcher(String pattern) {
        PathMatcher m = GLOBS.get(pattern);
        if (m == null) {
            try {
                m = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
            } catch (PatternSyntaxException | UnsupportedOperationException ex) {
                log.debug("Not a valid glob pattern, will try it as a regex: {}", pattern);
                m = NO_MATCH;
            }
            GLOBS.put(pattern, m);
        }
        return m == NO_MATCH ? null : m;
    }

    public static boolean ignored(String name) {
        if( name == null ) {
            return false; //indicates the root of Path
        }
        if( name.equals(".mil") ) { // usually ignore resources starting with a dot, but special case for .mil directory
            return false;
        }
        return name.startsWith(".");
    }

    public static boolean ignored(Path p) {
        while (p != null && p.getName() != null ) {
            if (Utils.ignored(p.getName())) {
                return true;
            }
            p = p.getParent();
        }
        return false;
    }

    public static Map<String, File> toMap(File[] files) {
        Map<String, File> map = new HashMap<>();
        if (files != null) {
            for (File r : files) {
                map.put(r.getName(), r);
            }
        }
        return map;
    }

    public static Map<String, ITriplet> toMap(List<ITriplet> triplets) {
        Map<String, ITriplet> map = new HashMap<>();
        if (triplets != null) {
            for (ITriplet r : triplets) {
                map.put(r.getName(), r);
            }
        }
        return map;
    }

    public static File toFile(File root, Path path) {
        File f = root;
        for (String fname : path.getParts()) {
            f = new File(f, fname);
        }
        return f;
    }

    public static String toType(File child) {
        return child.isDirectory() ? "d" : "f";
    }

}
