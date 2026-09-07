package co.kademi.sync.status;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the current status as JSON, for anything that already lives in a status bar to read:
 * waybar, polybar, i3status, tmux, SketchyBar, an editor extension.
 *
 * This is the sink that works everywhere, so it is the one that is always on. The tray icon is a
 * convenience on top of it.
 */
public class StatusFile implements StatusSink {

    private static final Logger log = LoggerFactory.getLogger(StatusFile.class);

    private final Path path;

    /**
     * Inside .ksync, which {@link io.milton.sync.Utils#ignored(File, java.util.List)} excludes
     * from the scan by name. A status file that was itself synced would push on every write and
     * then write again because it pushed.
     */
    public static Path defaultPath(File configDir) {
        return configDir.toPath().resolve("status.json");
    }

    public StatusFile(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    private boolean warned;

    @Override
    public void report(SyncStatus status) {
        try {
            write(status.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException ex) {
            // Once, at warn. A sync writes status on every change and a broken path would
            // otherwise bury the log in the same message.
            if (warned) {
                log.debug("Could not write status to {}", path, ex);
            } else {
                warned = true;
                log.warn("Could not write the status file {} - {}. Continuing without it", path, ex.getMessage());
            }
        }
    }

    /**
     * Written beside the target and renamed in, so a reader polling the file never catches it
     * half written and a crash leaves the previous status rather than a truncated file. Same
     * approach as the credential store.
     */
    private void write(byte[] data) throws IOException {
        Path dir = path.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        Path tmp = Files.createTempFile(dir, path.getFileName().toString() + "-", ".tmp");
        try {
            Files.write(tmp, data);
            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** A file cannot pop up a message; the state is in it already for anyone watching. */
    @Override
    public boolean alert(String title, String body, boolean problem) {
        return false;
    }

    /**
     * The last written status stays on disk deliberately. A reader starting up afterwards can see
     * how the last run ended, and the reporter writes STOPPED before closing.
     */
    @Override
    public void close() {
    }
}
