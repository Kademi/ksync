package io.milton.sync.triplets;

import io.milton.sync.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class FileSystemWatchingService {

    private static final Logger log = LoggerFactory.getLogger(FileSystemWatchingService.class);

    private static final WatchEvent.Kind<?>[] events = {StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY};

    private final WatchService watchService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Map<WatchNotificationListener, File> mapOfListeners;
    private final Map<WatchNotificationListener, List<String>> mapOfIgnores;
    private ScheduledFuture<?> futureScan;

    public FileSystemWatchingService(WatchService watchService, ScheduledExecutorService scheduledExecutorService) {
        this.watchService = watchService;
        this.mapOfListeners = new HashMap<>();
        this.mapOfIgnores = new HashMap<>();
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start() {

        Runnable rScan = () -> {
            try {
                scanFsEvents();
            } catch (IOException ex) {
                log.error("Exception processing events", ex);
            }
        };
        log.trace("Begin file watch loop");
        futureScan = scheduledExecutorService.scheduleWithFixedDelay(rScan, 200, 200, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (futureScan != null) {
            futureScan.cancel(true);
        }
    }

    void scanFsEvents() throws IOException {
        WatchKey watchKey;
        watchKey = watchService.poll(); // this call is blocking until events are present
        if (watchKey == null) {
            return;
        }

        Watchable w = watchKey.watchable();
        java.nio.file.Path watchedPath = (java.nio.file.Path) w;
        // poll for file system events on the WatchKey

        for (final WatchEvent<?> event : watchKey.pollEvents()) {
            // log.info("scanFsEvents: " + System.currentTimeMillis());
            WatchEvent.Kind<?> kind = event.kind();
            java.nio.file.Path p = (java.nio.file.Path) event.context();
            java.nio.file.Path pathCreated = (java.nio.file.Path) event.context();
            final File f = new File(watchedPath + File.separator + pathCreated);

            boolean ignored = false;
            for (Map.Entry<WatchNotificationListener, File> entry : mapOfListeners.entrySet()) {
                File dir = entry.getValue();
                if (f.getAbsolutePath().startsWith(dir.getAbsolutePath())) {
                    entry.getKey().onChange(kind, f);
                    ignored |= Utils.ignored(f, mapOfIgnores.get(entry.getKey()));
                }
            }

            // If this is a new directory we need to add a watch for it, unless it is ignored:
            // a node_modules created by npm install after startup must not be watched any more
            // than one that was there at startup
            if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
                if (f.isDirectory() && !ignored) {
                    registerWatchDir(f);
                }
            } else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
                if (f.isDirectory()) {
                    unregisterWatchDir(f);
                }
            }
        }

        // if the watched directed gets deleted, get out of run method
        if (!watchKey.reset()) {
            log.warn("Watch is no longer valid, changes to this path will stop being detected");
            watchKey.cancel();
        }
    }

    public List<WatchKey> watch(File dir, WatchNotificationListener listener) throws IOException {
        return watch(dir, listener, null);
    }

    /**
     * @param ignorePatterns directories matching these get no watch and are not descended into,
     * whether they exist now or are created while watching. Without them an ignored directory
     * still costs a watch per directory inside it: node_modules alone can hold tens of thousands,
     * which on linux exhausts the per user inotify limit and then the files that do matter
     * silently stop being watched. May be null.
     */
    public List<WatchKey> watch(File dir, WatchNotificationListener listener, List<String> ignorePatterns) throws IOException {
        mapOfListeners.put(listener, dir);
        mapOfIgnores.put(listener, ignorePatterns);
        List<WatchKey> watchKeys = new ArrayList<>();
        initWatch(dir, watchKeys, ignorePatterns);
        return watchKeys;
    }

    private void initWatch(File dir, List<WatchKey> watchKeys, List<String> ignorePatterns) throws IOException {
        if (dir.isDirectory()) {
            if (Utils.ignored(dir, ignorePatterns)) {
                log.debug("Not watching ignored directory {}", dir);
                return;
            }
            WatchKey key = registerWatchDir(dir);
            if( key != null ) {
                watchKeys.add(key);
            }
            File[] list = dir.listFiles();
            if (list != null) {
                for (File f : list) {
                    initWatch(f, watchKeys, ignorePatterns);
                }
            }
        }
    }

    private WatchKey registerWatchDir(final File dir) throws IOException {
        try {
            if (watchService == null) {
                return null;
            }

            final java.nio.file.Path path = FileSystems.getDefault().getPath(dir.getAbsolutePath());

            WatchKey key = path.register(watchService, events);
            //mapOfWatchKeysByDir.put(dir, key);
            log.debug("Watching: " + path);
            return key;
        } catch (Throwable ex) {
            throw new RuntimeException("Couldnt start watching dir: " + dir.getAbsolutePath(), ex);
        }

    }

    private void unregisterWatchDir(final File dir) {

    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }



    public interface WatchNotificationListener {

        void onChange(WatchEvent.Kind<?> event, File changed);
    }
}
