package co.kademi.sync;

import co.kademi.sync.oauth.CredentialStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The per-user ignore file, git's {@code core.excludesFile} layer and the
 * weakest of the three, so a checkout can always override it.
 *
 * Lives at ~/.config/ksync/ignore, beside the credentials file, which is where
 * git keeps its equivalent. Git does not read ~/.gitignore, and neither do we.
 *
 * Patterns are passed through untouched: {@link Ignores} is what understands
 * them.
 */
public class GlobalIgnores {

    private static final Logger log = LoggerFactory.getLogger(GlobalIgnores.class);

    /**
     * Overrides where the ignore file is kept. Set by tests so they cannot
     * touch the real one, and usable to relocate the file.
     */
    public static final String PATH_PROPERTY = "ksync.ignoreFile";

    private final Path path;

    public GlobalIgnores(Path path) {
        this.path = path;
    }

    /**
     * The conventional per-user location, ~/.config/ksync/ignore
     *
     * @return
     */
    public static GlobalIgnores defaultIgnores() {
        return new GlobalIgnores(defaultPath());
    }

    static Path defaultPath() {
        String override = System.getProperty(PATH_PROPERTY);
        if (StringUtils.isNotBlank(override)) {
            return Paths.get(override);
        }
        return CredentialStore.userConfigDir().resolve("ksync").resolve("ignore");
    }

    public Path getPath() {
        return path;
    }

    public boolean exists() {
        return Files.exists(path);
    }

    /**
     * @return the patterns in the file, in the order they appear, or an empty
     * list if there is no file. Never null, and never throws: a broken ignore
     * file must not stop a sync.
     */
    public List<String> patterns() {
        return readPatterns(path);
    }

    /**
     * Reads one ignore file. Blank lines and comments are left for the pattern
     * compiler to drop, so both files read the same way.
     */
    /**
     * Every line as it appears, so a line number reported by check-ignore
     * matches the file.
     */
    static List<String> readLines(Path file) {
        if (!Files.exists(file)) {
            return Collections.emptyList();
        }
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            log.warn("Could not read the ignore file {}, continuing without it", file, ex);
            return Collections.emptyList();
        }
    }

    static List<String> readPatterns(Path file) {
        if (!Files.exists(file)) {
            log.debug("No ignore file at {}", file);
            return Collections.emptyList();
        }
        try {
            List<String> lines = new ArrayList<>();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String s = StringUtils.stripEnd(line, " \t\r");
                if (!s.isEmpty() && !s.startsWith("#")) {
                    lines.add(s);
                }
            }
            return lines;
        } catch (IOException ex) {
            log.warn("Could not read the ignore file {}, continuing without it", file, ex);
            return Collections.emptyList();
        }
    }

    /**
     * Adds a pattern, creating the file if this is the first one.
     *
     * @return true if it was added, false if the file already had it
     */
    public boolean add(String pattern) throws IOException {
        String s = StringUtils.trimToNull(pattern);
        if (s == null || s.startsWith("#")) {
            throw new IllegalArgumentException("Not a usable ignore pattern: " + pattern);
        }
        if (patterns().contains(s)) {
            return false;
        }
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        // Append rather than rewrite, so any comments or ordering the user put in the file survive.
        // A file not ending in a newline would otherwise glue the new pattern onto the last one.
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
        byte[] all = Files.readAllBytes(path);
        if (all.length == 0) {
            return false;
        }
        byte last = all[all.length - 1];
        return last != '\n' && last != '\r';
    }
}
