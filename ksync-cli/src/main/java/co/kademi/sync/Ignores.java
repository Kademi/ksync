package co.kademi.sync;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What a sync leaves out, using gitignore's rules.
 *
 * Patterns match the path relative to the checkout root, not a bare name, which is what makes
 * anchoring and negation work. ksync-go reads the same file the same way, so a checkout behaves the
 * same whichever client touched it last.
 *
 * Replaces skipping every name starting with a dot: a dotfile is ordinary content, and the two
 * filters disagreed about it, so a dotfile on the branch could never scan back to its own hash.
 */
public class Ignores {

    private static final Logger log = LoggerFactory.getLogger(Ignores.class);

    /** The directory a checkout keeps its own state in, which is never syncable. */
    public static final String STATE_DIR = ".ksync";

    /**
     * Applied before any ignore file, so a later rule can undo one: a checkout that really does
     * keep its own node_modules says {@code !node_modules}. Same list as ksync-go, plus the two
     * ksync3 has always had; divergent defaults would give one checkout two behaviours.
     */
    public static final List<String> BUILT_IN = Collections.unmodifiableList(Arrays.asList(
            ".git/", ".svn/", ".hg/", ".DS_Store", "Thumbs.db", "node_modules", "bower_components"));

    /** The per-checkout ignore file, read from the root of the working tree. */
    public static final String IGNORE_FILE = ".ksyncignore";

    /** Where a rule came from, for check-ignore to report. */
    public static final String BUILT_IN_SOURCE = "built-in";
    public static final String RUN_SOURCE = "-ignore";

    private final List<Rule> rules;

    private Ignores(List<Rule> rules) {
        this.rules = rules;
    }

    /** Compiles patterns in order. Later wins, so a "!" rule re-includes, as in gitignore. */
    public static Ignores of(List<String> patterns) {
        List<Rule> rules = new ArrayList<>();
        addAll(rules, patterns, RUN_SOURCE);
        return new Ignores(rules);
    }

    /**
     * @param source what check-ignore reports as the origin of these rules: a file path, or one of
     * {@link #BUILT_IN_SOURCE} and {@link #RUN_SOURCE}
     */
    private static void addAll(List<Rule> rules, List<String> patterns, String source) {
        if (patterns == null) {
            return;
        }
        // Only a file has line numbers worth reporting; the built ins and -ignore have none
        boolean fromFile = !BUILT_IN_SOURCE.equals(source) && !RUN_SOURCE.equals(source);
        int line = 0;
        for (String pattern : patterns) {
            line++;
            Rule rule = Rule.compile(pattern, source, fromFile ? line : 0);
            if (rule != null) {
                rules.add(rule);
            }
        }
    }

    public static Ignores of(String... patterns) {
        return of(Arrays.asList(patterns));
    }

    /**
     * The layers git uses, weakest first: built ins, this user's file, the checkout's own, then
     * this run. Later wins throughout, so each layer can undo the one above it.
     *
     * @param root the checkout directory, which holds the shared ignore file
     * @param extra patterns from -ignore or ksync.properties, may be null
     */
    public static Ignores load(File root, List<String> extra) {
        List<Rule> rules = new ArrayList<>();
        addAll(rules, BUILT_IN, BUILT_IN_SOURCE);
        // Read with their blank and comment lines, so a reported line number matches the file
        GlobalIgnores user = GlobalIgnores.defaultIgnores();
        addAll(rules, GlobalIgnores.readLines(user.getPath()), user.getPath().toString());
        if (root != null) {
            File checkoutFile = new File(root, IGNORE_FILE);
            addAll(rules, GlobalIgnores.readLines(checkoutFile.toPath()), checkoutFile.getPath());
        }
        addAll(rules, extra, RUN_SOURCE);
        return new Ignores(rules);
    }

    /** Nothing excluded but the state directory. */
    public static Ignores none() {
        return new Ignores(Collections.<Rule>emptyList());
    }

    /**
     * Whether a path is excluded.
     *
     * @param relPath slash separated and relative to the checkout root
     * @param isDir whether it is a directory, which a directory-only rule needs to know
     */
    public boolean ignored(String relPath, boolean isDir) {
        Match match = explain(relPath, isDir);
        return match != null && match.ignored;
    }

    /**
     * The rule that decided a path, for check-ignore to report.
     *
     * @return the last rule to match, which is the one that wins, or null if none did
     */
    public Match explain(String relPath, boolean isDir) {
        if (StringUtils.isBlank(relPath) || ".".equals(relPath)) {
            return null;
        }
        String path = StringUtils.strip(relPath.replace('\\', '/'), "/");
        if (path.isEmpty()) {
            return null;
        }
        // Never syncable whatever the patterns say: it holds the object store and the remote ref
        if (path.equals(STATE_DIR) || path.startsWith(STATE_DIR + "/")) {
            return new Match(BUILT_IN_SOURCE, 0, STATE_DIR, true);
        }
        Rule winner = null;
        for (Rule rule : rules) {
            if (rule.matches(path, isDir)) {
                winner = rule;
            }
        }
        return winner == null ? null : new Match(winner.source, winner.line, winner.pattern, !winner.negate);
    }

    /** Which rule decided a path, and where it came from. */
    public static final class Match {

        public final String source;
        /** 1 based, or 0 when the source is not a file. */
        public final int line;
        public final String pattern;
        /** False when a "!" rule won, which re-includes the path. */
        public final boolean ignored;

        Match(String source, int line, String pattern, boolean ignored) {
            this.source = source;
            this.line = line;
            this.pattern = pattern;
            this.ignored = ignored;
        }

        @Override
        public String toString() {
            return line > 0 ? source + ":" + line + ":" + pattern : source + ":" + pattern;
        }
    }

    /**
     * One compiled pattern. {@code under} matches the pattern and anything below it, {@code exact}
     * only the pattern itself, and a directory-only rule needs both.
     */
    private static final class Rule {

        private final Pattern under;
        private final Pattern exact;
        private final boolean negate;
        private final boolean dirOnly;
        private final String source;
        private final int line;
        private final String pattern;

        private Rule(Pattern under, Pattern exact, boolean negate, boolean dirOnly,
                String source, int line, String pattern) {
            this.under = under;
            this.exact = exact;
            this.negate = negate;
            this.dirOnly = dirOnly;
            this.source = source;
            this.line = line;
            this.pattern = pattern;
        }

        boolean matches(String path, boolean isDir) {
            if (!under.matcher(path).matches()) {
                return false;
            }
            // "build/" still covers everything below it, so only an exact hit must be a directory
            if (dirOnly && !isDir && exact.matcher(path).matches()) {
                return false;
            }
            return true;
        }

        /** @return the rule, or null for a pattern that selects nothing, such as a bare "!" or "/" */
        static Rule compile(String rawPattern, String source, int line) {
            if (rawPattern == null) {
                return null;
            }
            String pattern = rawPattern.trim();
            if (pattern.isEmpty() || pattern.startsWith("#")) {
                return null;
            }
            boolean negate = false;
            if (pattern.startsWith("\\")) {
                pattern = pattern.substring(1); // "\#notacomment", "\!literal"
            } else if (pattern.startsWith("!")) {
                negate = true;
                pattern = pattern.substring(1);
            }

            boolean dirOnly = false;
            if (pattern.endsWith("/")) {
                dirOnly = true;
                pattern = StringUtils.removeEnd(pattern, "/");
            }
            if (pattern.isEmpty()) {
                return null;
            }

            // A separator at the start or middle anchors to the root; a bare name matches at any
            // depth. The trailing slash removed above does not count, so this is tested after it.
            boolean anchored = pattern.startsWith("/") || StringUtils.removeStart(pattern, "/").contains("/");
            pattern = StringUtils.removeStart(pattern, "/");
            if (pattern.isEmpty()) {
                return null;
            }

            String body = globToRegex(pattern);
            String prefix = anchored ? "^" : "^(?:.*/)?";
            try {
                return new Rule(Pattern.compile(prefix + body + "(?:/.*)?$"),
                        Pattern.compile(prefix + body + "$"), negate, dirOnly,
                        source, line, rawPattern.trim());
            } catch (PatternSyntaxException ex) {
                // one bad line must not stop a sync
                log.warn("Skipping ignore pattern that will not compile: {}", rawPattern);
                return null;
            }
        }
    }

    /** A regex rather than a PathMatcher, which cannot express "at any depth". */
    static String globToRegex(String glob) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        i++;
                        if (i + 1 < glob.length() && glob.charAt(i + 1) == '/') {
                            i++; // "**/" is zero or more directories, so "**/foo" matches "foo"
                            b.append("(?:[^/]+/)*");
                        } else {
                            b.append(".*");
                        }
                    } else {
                        b.append("[^/]*"); // a single star stops at a separator
                    }
                    break;
                case '?':
                    b.append("[^/]");
                    break;
                case '[': {
                    int width = charClassWidth(glob, i);
                    if (width > 0) {
                        b.append(charClass(glob.substring(i, i + width)));
                        i += width - 1;
                    } else {
                        b.append("\\["); // an unterminated class is a literal bracket, as in git
                    }
                    break;
                }
                default:
                    b.append(quote(c));
                    break;
            }
        }
        return b.toString();
    }

    /** @return how many characters the bracket expression at i spans, or 0 if it is not one */
    private static int charClassWidth(String glob, int i) {
        int end = glob.indexOf(']', i + 1);
        if (end < 0) {
            return 0;
        }
        String inner = glob.substring(i + 1, end);
        // an empty class selects nothing, and a separator would let it cross directories
        if (inner.isEmpty() || inner.contains("/")) {
            return 0;
        }
        return end - i + 1;
    }

    /** Copies a bracket expression through, turning gitignore's "!" negation into the regex "^". */
    private static String charClass(String bracketed) {
        String inner = bracketed.substring(1, bracketed.length() - 1);
        if (inner.startsWith("!")) {
            inner = "^" + inner.substring(1);
        }
        // && is an intersection in a java character class, and means nothing in gitignore
        return "[" + inner.replace("&&", "\\&\\&") + "]";
    }

    private static String quote(char c) {
        return "\\^$.|?*+()[]{}".indexOf(c) >= 0 ? "\\" + c : String.valueOf(c);
    }
}
