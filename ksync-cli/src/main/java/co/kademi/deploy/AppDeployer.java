/*
 *       Copyright Kademi
 */
package co.kademi.deploy;

import co.kademi.sync.KSync3Utils;
import co.kademi.sync.KSyncUtils;
import static co.kademi.sync.KSyncUtils.writeLoginProps;
import io.milton.common.Path;
import io.milton.event.EventManagerImpl;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import io.milton.httpclient.HttpResult;
import io.milton.httpclient.PropFindResponse;
import io.milton.httpclient.RespUtils;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.MinimalPutsBlobStore;
import io.milton.sync.MinimalPutsHashStore;
import io.milton.sync.triplets.BerkeleyDbFileHashCache;
import io.milton.sync.triplets.BlockingBlobStore;
import io.milton.sync.triplets.BlockingHashStore;
import io.milton.sync.triplets.FileSystemWatchingService;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import io.milton.sync.triplets.SyncHashCache;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class AppDeployer {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AppDeployer.class);

    public static final String DEFAULT_VERSION = "1.0.0";

    public static String findVersion(File appDir) {
        File versionFile = new File(appDir, "app-version.txt");
        if (versionFile.exists()) {
            try {
                String version = FileUtils.readFileToString(versionFile);

                version = version == null ? null : version.trim();

                if (StringUtils.isEmpty(version)) {
                    log.error("findVersion: Version file " + versionFile.getAbsolutePath() + " is empty, assuming default 0001 version");

                    return DEFAULT_VERSION;
                }

                return version;
            } catch (IOException ex) {
                log.error("Couldnt read version file " + versionFile.getAbsolutePath());
            }

        }
        return DEFAULT_VERSION;
    }

    public static void incrementVersionNumber(File appDir, String versionName) {
        String s = getIncrementedVersionNumber(versionName);
        File versionFile = new File(appDir, "app-version.txt");
        try {
            FileUtils.write(versionFile, s);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getIncrementedVersionNumber(String versionName) {
        String[] arr = versionName.split("[.]");
        int part = Integer.parseInt(arr[arr.length - 1]);
        part++;
        String result = "";
        for (int i = 0; i < arr.length - 1; i++) {
            result += arr[i] + ".";
        }
        return result + part;
    }

    public static void publish(Options options, CommandLine line) throws Exception {
        log.info("Running publish command..");

        KSyncUtils.withDir((File dir) -> {
            if (!dir.exists()) {
                System.out.println("Dir not found: " + dir.getAbsolutePath());
                return;
            }

            File configDir = new File(dir, ".ksync");
            configDir.mkdirs();
            Properties props = KSyncUtils.readProps(configDir);

            String auth = line.getOptionValue("auth");
            if (StringUtils.isNotBlank(auth)) {
                if (auth.contains(",")) {
                    String[] arr = auth.split(",");
                    String userName = arr[0].trim();
                    String token = arr[1].trim();
                    String userUrl = "/users/" + userName;
                    writeLoginProps(userUrl, token, configDir);
                    props = KSyncUtils.readProps(configDir);
                }
            }

            Map cookies = KSyncUtils.getCookies(configDir);
            String url = KSync3Utils.getInput(options, line, "url", props, true);
            log.info(url);
            String user = KSync3Utils.getInput(options, line, "user", props, cookies.isEmpty());
            String password = null;
            if (cookies.isEmpty()) {
                password = KSync3Utils.getPassword(line, user, url);
            }
            String appIds = KSync3Utils.getInput(options, line, "appids", null);

            String sIgnores = KSync3Utils.getInput(options, line, "ignore", props, false);
            List<String> ignores = KSync3Utils.split(sIgnores);

            AppDeployer d;
            try {
                d = new AppDeployer(dir, url, user, password, appIds, cookies);
                d.autoIncrement = KSync3Utils.getBooleanInput(line, "versionincrement");
                d.force = KSync3Utils.getBooleanInput(line, "force");
                d.report = KSync3Utils.getBooleanInput(line, "report");
                d.ignores = ignores;

                log.info("---- OPTIONS ----");
                log.info("url: " + url);
                log.info("dir: " + dir.getAbsolutePath());
                log.info("  -autoincrement: " + d.autoIncrement);
                log.info("  -report: " + d.report);
                log.info("  -force: " + d.force);
                log.info("  -appIds: " + appIds);
                log.info("  -ignore: " + ignores);
                log.info("--------------");
                d.upsync();

                if (!d.results.errors.isEmpty()) {
                    System.exit(1);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            log.info("Completed");
        }, options, line);
    }

    private final File rootDir;
    private final Host client;
    private final List<String> appIds;
    private final Results results = new Results();

    private HttpBlobStore httpBlobStore;
    private HttpHashStore httpHashStore;

    private final BlobStore localBlobStore;
    private final HashStore localHashStore;

    private boolean autoIncrement = false; // if true, update version numbers in files
    private boolean report; // if true, dont make any changes
    private boolean force;
    private List<String> ignores;
    private final ScheduledExecutorService scheduledExecutorService;
    private final FileSystemWatchingService fileSystemWatchingService;
    private final SyncHashCache fileHashCache;

    public AppDeployer(File dir, String sRemoteAddress, String user, String password, String sAppIds, Map<String, String> cookies) throws MalformedURLException {
        this.rootDir = dir;

        URL url = new URL(sRemoteAddress);
        int timeout = 180000;
        //client = new Host(url.getHost(), url.getPort(), user, password, null);
        client = new Host(url.getHost(), "/", url.getPort(), user, password, null, timeout, null, null);
        if (cookies != null) {
            client.getCookies().putAll(cookies);
            client.setUsePreemptiveAuth(false);
        }
        client.setUseDigestForPreemptiveAuth(false);
        boolean secure = url.getProtocol().equals("https");
        client.setSecure(secure);
        this.appIds = KSync3Utils.split(sAppIds);

        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File localDataDir = new File(tmpDir, "appDeployer");
        String dataDirName = "appDeployer-filecache-" + client.server;
        if (StringUtils.isNotBlank(user)) {
            dataDirName += "-" + user;
        }
        File envDir = new File(tmpDir, dataDirName);
        log.info("Using local data dir {}", localDataDir);

        localBlobStore = new FileSystem2BlobStore(new File(localDataDir, "blobs"));
        localHashStore = new FileSystem2HashStore(new File(localDataDir, "hash"));

        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        final java.nio.file.Path path = FileSystems.getDefault().getPath(rootDir.getAbsolutePath());
        WatchService watchService = null;
        try {
            watchService = path.getFileSystem().newWatchService();
        } catch (IOException ex) {
            log.error("Exception initialising watch service");
        }
        if (watchService != null) {
            fileSystemWatchingService = new FileSystemWatchingService(watchService, scheduledExecutorService);
        } else {
            fileSystemWatchingService = null;
        }
        fileHashCache = new BerkeleyDbFileHashCache(envDir);
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void upsync() throws IOException {

        try {
            this.client.doOptions(Path.root);
        } catch (NotAuthorizedException nae) {
            log.error("Not authorised to access server. Please check your user name and password");
            return;
        } catch (Exception ex) {
            log.error("Exeption connecting to server. Please check connection details", ex);
            return;
        }

        upSyncMarketplaceDir(new File(rootDir, "themes"), true, false);
        upSyncMarketplaceDir(new File(rootDir, "apps"), false, true);
        upSyncMarketplaceDir(new File(rootDir, "libs"), false, false);
        upSyncMarketplaceDir(new File(rootDir, "recipes"), false, false);

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("-------- RESULTS -------------");
        System.out.println("-- INFO -- ");
        for (String s : results.infos) {
            System.out.println(s);
        }
        System.out.println("-- WARNINGS -- ");
        for (String s : results.warnings) {
            System.out.println(s);
        }
        System.out.println("-- ERRORS -- ");
        for (String s : results.errors) {
            System.out.println(s);
        }
        System.out.println("");
        System.out.println("");
        System.out.println("");
    }

    private void upSyncMarketplaceDir(File dir, boolean isTheme, boolean isApp) throws IOException {
        log.info("upsync {} {} {}", dir, isTheme, isApp);
        if (dir.listFiles() == null) {
            log.warn("No child dirs in " + dir.getAbsolutePath());
            return;
        }

        // Note that by providing a watchservice, MemoryLocalTripletStore will not create a watcher.
        final java.nio.file.Path path = FileSystems.getDefault().getPath(dir.getAbsolutePath());
        WatchService watchService = path.getFileSystem().newWatchService();
        FileSystemWatchingService fileWatchService = new FileSystemWatchingService(watchService, scheduledExecutorService);

        for (File appDir : dir.listFiles()) {
            if (appDir.isDirectory()) {
                String appName = appDir.getName();

                if (isProcess(appDir)) {
                    processAppDir(appName, isTheme, isApp, appDir, fileWatchService);
                }
            }
        }
    }

    private void processAppDir(String appName, boolean isTheme, boolean isApp, File appDir, FileSystemWatchingService fileWatchService) throws RuntimeException {
        log.info("checkCreateApp {} {}", appName);
        String appPath = "/manageApps/" + appName;
        boolean appCreated = false;
        if (!doesExist(appPath)) {
            if (createApp(appName, isTheme, isApp)) {
                appCreated = true;
                log.info("created app {}", appName);
            } else {
                results.errors.add(appName + " - couldnt create app");
                return;
            }
        }

        String versionName = AppDeployer.findVersion(appDir);
        String appVersionPath = "/repositories/" + appName + "/" + versionName + "/";
        boolean doesVersionExist = doesExist(appVersionPath);

        if (doesVersionExist) {
            if (force) {
                log.info("App version is already published " + appVersionPath + " but force is on so will continue..");
            } else {
                log.info("App version is already published " + appVersionPath + ", so ignoring");
                return;
            }

            // Check that local and remote hashes match, and warn if they dont
            String localHash = getLocalHash(appName, versionName, appDir);
            if (localHash == null) {
                log.warn("Could not get local hash for " + appDir);
                if (!force) {
                    results.errors.add(appName + " version " + versionName + " is already published, but could not find local hash to compare");
                    return;
                }
            } else {
                String branchPath = "/repositories/" + appName + "/" + versionName + "/";
                String remoteHash = getRemoteHash(branchPath);
                if (localHash.equals(remoteHash)) {
                    if (!force) {
                        log.info("App is an exact match local and remote ={}", localHash);
                        results.infos.add(appName + " version " + versionName + " is already published, and exactly matches local " + localHash);
                        return;
                    } else {
                        log.warn("App is the same as published version, but force is true so will republish. app={} version={} local={} remote={}", appName, versionName, localHash, remoteHash);
                    }
                } else {
                    log.warn("App is not the same as published version so will republish. app={} version={} local={} remote={}", appName, versionName, localHash, remoteHash);
                }
            }
        }

        String localHash = upSyncMarketplaceVersionDir(appName, versionName, appDir, fileWatchService);
        if (localHash != null) {

            if (appCreated) {
                if (!addToMarketPlace(appName)) {
                    results.errors.add(appName + " Did not add app to marketplace " + appPath);
                    return;
                }
            }

            String branchPath = "/repositories/" + appName + "/";

            if (report) {
                results.infos.add(appName + " - Would have published version " + branchPath + "/" + versionName + " with local hash " + localHash);
                return;
            } else {
                if (makeCurrentVersionLive(branchPath, versionName)) {
                    // Republic, so the live version is the published version
                    if (!publishApp(appName)) {
                        results.errors.add(appName + "Pushed, but could not (re)publish app to marketplace " + appPath);
                        return;
                    }

                    if (autoIncrement) {
                        if (report) {
                            log.info("Would have auto-incremented " + appDir);
                        } else {
                            incrementVersionNumber(appDir, versionName);
                        }
                    }

                    results.infos.add(appName + " - Published " + appName + " version " + versionName + " with hash " + localHash);
                    return;
                } else {
                    results.errors.add(appName + " - Failed to publish version " + appName);
                    return;
                }
            }

        } else {
            results.errors.add(appName + " Failed to sync local to remote " + appName);
            return;
        }

    }

    private String upSyncMarketplaceVersionDir(String appName, String versionName, File localRootDir, FileSystemWatchingService fileWatchService) {
        log.info("upSyncMarketplaceVersionDir app={}", appName);
        try {
            if (!checkCreateAppVersion(appName, versionName)) {
                return null;
            }

            String branchPath = "/repositories/" + appName + "/" + versionName + "/";

            HttpBloomFilterHashCache blobsHashCache = null;
            HttpBloomFilterHashCache chunckFanoutHashCache = null;
            HttpBloomFilterHashCache fileFanoutHashCache = null;
            if (!report) {
                blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
                chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");
                fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");
            }

            httpBlobStore = new HttpBlobStore(client, blobsHashCache);
            httpBlobStore.setBaseUrl("/_hashes/blobs/");

            httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
            httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
            httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

            MinimalPutsBlobStore mpBlobStore = new MinimalPutsBlobStore(httpBlobStore);
            MinimalPutsHashStore mpHashStore = new MinimalPutsHashStore(httpHashStore);

            BlobStore blobStore;
            HashStore hashStore;
            if (report) {
                blobStore = localBlobStore;
                hashStore = localHashStore;
            } else {
                blobStore = new AppDeployerBlobStore(client, localBlobStore, mpBlobStore, true);
                hashStore = new AppDeployerHashStore(client, localHashStore, mpHashStore);
            }

            AtomicBoolean needsPush = new AtomicBoolean();
            MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), blobStore, hashStore, (String rootHash) -> {
                needsPush.set(true);
            }, null, fileWatchService, ignores, fileHashCache);

            String newHash = s.scan();

            if (blobStore instanceof BlockingBlobStore) {
                BlockingBlobStore bbs = (BlockingBlobStore) blobStore;
                bbs.checkComplete();
            }
            if (hashStore instanceof BlockingHashStore) {
                BlockingHashStore bhs = (BlockingHashStore) hashStore;
                bhs.checkComplete();
            }

            log.info("HttpBlobStore: gets={} sets={}", httpBlobStore.getGets(), httpBlobStore.getSets());
            log.info("HttpHashStore: gets={} sets={}", httpHashStore.getGets(), httpHashStore.getSets());

            if (needsPush.get() || force) {
                try {
                    if (needsPush.get()) {
                        log.info("File changed in {}, new repo hash {}", localRootDir, newHash);
                    } else {
                        log.info("No file changes detected, but force is on so will push, repo hash {}", localRootDir, newHash);
                    }
                    push(appName, newHash, branchPath);

                } catch (Exception ex) {
                    log.error("Exception in file changed event handler", ex);
                    return null;
                }
            }

            return newHash;
        } catch (Exception ex) {
            log.error("Exception upsyncing " + appName, ex);
            return null;
        }
    }

    private void push(String appName, String localRootHash, String branchPath) {
        String remoteHash = getRemoteHash(branchPath);
        if (remoteHash == null) {
            log.info("Aborted");
            return;
        }
        if (remoteHash.equals(localRootHash)) {
            if (!force) {
                log.info("No change. Local repo is exactly the same as remote hash={}", localRootHash);
                return;
            }
        }
        if (report) {
            log.info("Not doing push {} because in report mode", branchPath);
            return;
        }

        // Now set the hash on the repo, and check for any missing objects
        Map<String, String> params = new HashMap<>();
        params.put("newHash", localRootHash);
        params.put("validate", "async");
        try {
            log.info("PUSH Local: {} Remote: {}", localRootHash, remoteHash);
            String res = client.post(branchPath, params);

            // Get the JobID to poll with
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Push async started ok");
                    JSONObject data = (JSONObject) jsonRes.get("data");
                    Long jobId = asLong(data.get("jobId"));

                    pollForPushComplete(jobId, appName, localRootHash, branchPath);
                } else {
                    results.errors.add(appName + " - EXCEPTION: Failed to push to " + branchPath + ", got negative status from async push request");
                }
            } else {
                results.errors.add(appName + " - EXCEPTION: Failed to push to " + branchPath + ", got no status from async push request");
            }

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            results.errors.add(appName + " - EXCEPTION: Failed to push to " + branchPath + " because " + ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    private String getRemoteHash(String path) {
        try {
            byte[] resp = client.get(path + "/?type=hash");
            String s = new String(resp);
            return s;
        } catch (HttpException | NotAuthorizedException | BadRequestException | ConflictException | NotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean checkCreateAppVersion(String appName, String versionName) throws NotAuthorizedException, UnknownHostException, SocketTimeoutException, IOException, ConnectException, HttpException {
        // Now check for the version
        String appBasPath = "/repositories/" + appName + "/";
        String versionPath = appBasPath + versionName;
        if (!doesExist(versionPath)) {
            log.info("Version does not exist app={} version={}", appName, versionName);
            if (createVersion(appBasPath, versionName)) {
                log.info("Created version {}", versionName);
            } else {
                if (report) {
                    results.infos.add(appName + " - Would have created " + versionName + " because that version doesnt exist");
                    return false;
                } else {
                    throw new RuntimeException("Couldnt create version " + versionPath);
                }
            }
        } else {
            log.info("Version already exists app={} version={}", appName, versionName);
        }
        return true;
    }

    private boolean doesExist(String path) {
        try {
            try {
                client.options(path);
                // exists, all good
                return true;
            } catch (NotFoundException ex) {
                return false;
            }
        } catch (NotAuthorizedException | IOException | HttpException ex) {
            throw new RuntimeException(ex);
        }

    }

    private boolean createApp(String appName, boolean isTheme, boolean isApp) {
        if (report) {
            log.info("Not doing create app {} because in report mode", appName);
            return false;
        }
        Map<String, String> params = new HashMap<>();
        params.put("newAppName", appName);
        params.put("newTitle", appName);
        params.put("providesTheme", isTheme + "");
        params.put("providesApp", isApp + "");
        try {
            log.info("createApp {}", appName);
            String res = client.post("/manageApps/", params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Created ok");
                    return true;
                }
            }
            log.info("Create app failed", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new RuntimeException("Exception creating app " + appName, ex);
        }
    }

    private boolean createVersion(String appBasPath, String versionName) {
        if (report) {
            log.info("Not doing create version {}/{} because in report mode", appBasPath, versionName);
            return false;
        }
        try {
            Path p = Path.path(appBasPath);
            String appPath = p + "/";
            List<PropFindResponse> list = client.propFind(appPath, 1, RespUtils.davName("name"), RespUtils.davName("resourcetype"), RespUtils.davName("iscollection"));
            List<String> versions = new ArrayList<>();
            log.info("createVersion: looking for highest version, found {} child resources", list.size());
            for (PropFindResponse l : list) {
                if (l.isCollection()) {
                    String name = l.getName();
                    if (!name.equals("live-videos") && !name.startsWith("version")) {
                        versions.add(name);
                    }
                }
            }
            String highestVersion;
            if (versions.isEmpty()) {
                log.info("createVersion: no existing versions, use default 'version1', tried propfind path {}", appPath);
                highestVersion = "version1";
            } else {
                versions.sort(ComparatorUtils.NATURAL_COMPARATOR);
                highestVersion = versions.get(versions.size() - 1);
            }
            log.info("Found highest version {} of app {}", highestVersion, appBasPath);

            String version1 = appBasPath + highestVersion;
            Map<String, String> params = new HashMap<>();
            params.put("copyToName", versionName);

            log.info("createVersion {} -> {}", version1, versionName);
            String res = client.post(version1, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Created ok");
                    return true;
                }
            }
            log.info("Create version failed", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException | IOException ex) {
            throw new RuntimeException("Exception creating app " + versionName, ex);
        }

    }

    private boolean addToMarketPlace(String appName) {
        log.info("addToMarketPlace: {}", appName);
        if (report) {
            log.info("Not doing addToMarketPlace {} because in report mode", appName);
            return false;
        }
        // http://localhost:8080/manageApps/test1/
        String pubPath = "/manageApps/" + appName + "/";
        Map<String, String> params = new HashMap<>();
        params.put("createMarketItem", "true");
        params.put("uniqueName", appName);
        try {
            String res = client.post(pubPath, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Published ok");
                    return true;
                }
            }
            log.info("add to market place failed", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new RuntimeException("Exception adding app to marketplace" + appName, ex);
        }
    }

    private boolean publishApp(String appName) {
        log.info("publishApp: {}", appName);
        if (report) {
            log.info("Not doing publishApp {} because in report mode", appName);
            return false;
        }
        // http://localhost:8080/manageApps/test1/
        String pubPath = "/manageApps/" + appName + "/";
        Map<String, String> params = new HashMap<>();
        params.put("publishApp", "true");
        try {
            String res = client.post(pubPath, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Published ok");
                    return true;
                }
            }
            log.info("add to market place failed - {}", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new RuntimeException("Exception publishing app to marketplace" + appName, ex);
        }
    }

    /**
     * Make this the live branch
     *
     * @param appBasPath
     * @param versionName
     * @return
     */
    private boolean makeCurrentVersionLive(String appBasPath, String versionName) {
        log.info("publishVersion: app path={} version={}", appBasPath, versionName);
        if (report) {
            log.info("Not doing publishVersion {}/{} because in report mode", appBasPath, versionName);
            return false;
        }
        // http://localhost:8080/repositories/test1/1.0.0/publish
        String pubPath = appBasPath + "/" + versionName + "/publish";
        Map<String, String> params = new HashMap<>();
        try {
            log.info("publishVersion {} -> {}", appBasPath, versionName);
            String res = client.post(pubPath, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Published ok");
                    return true;
                }
            }
            log.info("Create version failed", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new RuntimeException("Exception publishing app " + versionName, ex);
        }
    }

    private boolean isProcess(File appDir) {
        if (appIds == null || appIds.isEmpty()) {
            return true;
        }
        for (String s : appIds) {
            if (s.startsWith("/")) {
                String dirName = s.substring(1);
                if (appDir.getParentFile().getName().equals(dirName)) {
                    return true;
                }
            }
            if (s.equals("*")) {
                return true;
            }
            if (s.equals(appDir.getName())) {
                return true;
            }
        }
        return false;
    }

    private String getLocalHash(String appName, String versionName, File localRootDir) {
        log.info("getLocalHash app={}", appName);
        try {
            // Dont need bloom filters because we wont be pushing
            HttpBloomFilterHashCache blobsHashCache = null;
            HttpBloomFilterHashCache chunckFanoutHashCache = null;
            HttpBloomFilterHashCache fileFanoutHashCache = null;

            String branchPath = "/repositories/" + appName + "/" + versionName + "/";
            if (!report) {
                blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
                chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");
                fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");
            }

            httpBlobStore = new HttpBlobStore(client, blobsHashCache);
            httpBlobStore.setBaseUrl("/_hashes/blobs/");

            httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
            httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
            httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

            MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), localBlobStore, localHashStore, (String rootHash) -> {
            }, null, fileSystemWatchingService, ignores, fileHashCache);
            return s.scan();
        } catch (Exception ex) {
            log.error("Could not find local hash for " + localRootDir, ex);
            return null;
        }
    }

    private void pollForPushComplete(Long jobId, String appName, String localRootHash, String branchPath) {
        Map<String, String> params = new HashMap<>();
        long sleepyTime = 100;
        PollJobResult pollRes = null;
        try {
            log.info("PUSH Local: {}", localRootHash);
            boolean done = false;
            while (!done) {
                String url = "/tasks/?jobId=" + jobId + "&asJson";
                log.info("poll for push result {} ...", url);
                byte[] bytes = client.get(url); // response can either be in-progress, or completed. If completed will have missing objects in data
                String res = new String(bytes);
                pollRes = parseJson(res);
                if (pollRes.isCancelled()) {
                    throw new RuntimeException("Verify and push task was cancelled: " + jobId);
                } else if (pollRes.isCompleted()) {
                    done = true;
                } else {
                    if (sleepyTime < 1500) {
                        sleepyTime += 10;
                    }
                    try {
                        Thread.sleep(sleepyTime);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Interrupted", ex);
                    }
                }
            }
            if (pollRes == null) {
                throw new RuntimeException("Couldnt get a poll result");
            }
            processPushResponse(pollRes.getJobOutput(), appName, localRootHash, branchPath, jobId);

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            results.errors.add(appName + " - EXCEPTION: Failed to push to " + branchPath + " because " + ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    private void processPushResponse(String res, String appName, String localRootHash, String branchPath, Long jobId) {
        JSONObject jsonRes = JSONObject.fromObject(res);

        JSONObject data = jsonRes;
        if (data != null) {
            JSONArray missingFileFanouts = getArray(data, "missingFileFanouts");    // if empty returns null
            JSONArray missingChunkFanouts = getArray(data, "missingChunkFanouts");
            JSONArray missingBlobs = getArray(data, "missingBlobs");

            if (missingBlobs != null || missingChunkFanouts != null || missingFileFanouts != null) {
                log.info("processPushResponse: missing objects, will upload individually");
                int count = 0;
                httpHashStore.setForce(true);
                httpBlobStore.setForce(true);
                if (missingFileFanouts != null) {
                    for (Object ff : missingFileFanouts.toArray()) {
                        log.info("Missing file fanout: {}", ff);
                        String missingHash = ff.toString();
                        Fanout f = localHashStore.getFileFanout(missingHash);
                        if (f == null) {
                            throw new RuntimeException("Could not find locally missing file fanout: " + missingHash);
                        }
                        count++;
                        httpHashStore.setFileFanout(missingHash, f.getHashes(), f.getActualContentLength());
                        log.info("Uploaded missing file fanout");
                    }
                }

                if (missingChunkFanouts != null) {
                    for (Object ff : missingChunkFanouts.toArray()) {
                        log.info("Missing chunk fanout: {}", ff);
                        String missingHash = ff.toString();
                        Fanout f = localHashStore.getChunkFanout(missingHash);
                        if (f == null) {
                            throw new RuntimeException("Could not find locally missing chunk fanout: " + missingHash);
                        }
                        count++;
                        httpHashStore.setChunkFanout(missingHash, f.getHashes(), f.getActualContentLength());
                        log.info("Uploaded missing chunk fanout");
                    }
                }

                // missingBlobs
                if (missingBlobs != null) {
                    for (Object ff : missingBlobs.toArray()) {
                        log.info("Missing blob: {}", ff);
                        String missingHash = ff.toString();
                        byte[] f = localBlobStore.getBlob(missingHash);
                        if (f == null) {
                            throw new RuntimeException("Could not find locally missing blob: " + missingHash);
                        }
                        httpBlobStore.setBlob(missingHash, f);
                        count++;
                        log.info("Uploaded missing blob");
                    }
                }
                log.info("Push failed: But uploaded " + count + "missing objects have been uploaded so will try again :)", res);
                push(appName, localRootHash, branchPath);
                return;
            }
        }

        log.info("Push and verify job {} complete, no missing objects, repository {} has been updated", jobId, appName);
    }

    private PollJobResult parseJson(String res) {
        JSONObject jsonRes = JSONObject.fromObject(res);
        boolean completed = asBool(jsonRes.get("completed"));
        boolean cancelled = asBool(jsonRes.get("cancelled"));
        String jobOutput = null;
        if (jsonRes.has("jobOutput")) {
            jobOutput = jsonRes.getString("jobOutput");
        }
        return new PollJobResult(completed, cancelled, jobOutput);

    }

    private boolean asBool(Object ob) {
        if (ob == null) {
            return false;
        } else {
            return Boolean.parseBoolean(ob.toString());
        }
    }

    private Long asLong(Object v) {
        if (v == null) {
            return null;
        } else if (v instanceof Integer) {
            Integer i = (Integer) v;
            return i.longValue();
        } else if (v instanceof Long) {
            Long l = (Long) v;
            return l;
        } else {
            String s = v.toString();
            return Long.parseLong(s);
        }
    }

    private JSONArray getArray(JSONObject data, String name) {
        if (data.containsKey(name)) {
            JSONArray arr = (JSONArray) data.get(name);
            if (arr.size() > 0) {
                return arr;
            }
        }
        return null;
    }

    public class PollJobResult {

        private boolean completed;
        private boolean cancelled;
        private String jobOutput;

        public PollJobResult(boolean completed, boolean cancelled, String jobOutput) {
            this.completed = completed;
            this.cancelled = cancelled;
            this.jobOutput = jobOutput;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isCompleted() {
            return completed;
        }

        public String getJobOutput() {
            return jobOutput;
        }

    }

    public class AppDeployerBlobStore implements BlockingBlobStore {

        private final Host host;
        private final BlobStore local;
        private final BlobStore remote;
        private final boolean bulkUpload;
        private final BlockingQueue<BlobImpl> blobs = new ArrayBlockingQueue<>(1000);
        private final Counter transferQueueCounter = new Counter();

        private boolean running = true;
        private Exception transferException;
        private final LinkedBlockingQueue<Runnable> transferJobs = new LinkedBlockingQueue<>(100);
        private final ThreadPoolExecutor.CallerRunsPolicy rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        private final ExecutorService transferExecutor = new ThreadPoolExecutor(5, 5, 60, TimeUnit.SECONDS, transferJobs, rejectedExecutionHandler);
        private final Thread queueProcessor;

        public AppDeployerBlobStore(Host host, BlobStore local, BlobStore remote, boolean bulkUpload) {
            this.host = host;
            this.local = local;
            this.remote = remote;
            this.bulkUpload = bulkUpload;

            queueProcessor = new Thread(() -> {
                try {
                    while (!blobs.isEmpty() || running) {
                        Set<BlobImpl> toUpload = new HashSet<>();
                        this.blobs.drainTo(toUpload, 100);
                        if (!toUpload.isEmpty()) {
                            doBulkUpload(toUpload);
                        } else {
                            Thread.sleep(300);
                        }
                    }
                    log.info("AppDeployerBlobStore: transfer process finished");
                } catch (Exception ex) {
                    log.error("AppDeployerBlobStore: exception", ex);
                    this.transferException = ex;
                }
            });
            queueProcessor.start();
        }

        @Override
        public void setBlob(String hash, byte[] bytes) {
            if (!local.hasBlob(hash)) {
                local.setBlob(hash, bytes);
            }
            //remote.setBlob(hash, bytes);

            if (bulkUpload) {
                if (!running) {
                    throw new RuntimeException("Store is already stopped");
                }
                try {
                    blobs.put(new BlobImpl(hash, bytes));
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                transferQueueCounter.up("blob");
                transferExecutor.submit(() -> {
                    log.info("setBlob: start transfer. Count={}", transferQueueCounter.count());
                    long tm = System.currentTimeMillis();
                    try {
                        //System.out.println("upload " + dirHash);
                        remote.setBlob(hash, bytes);
                        //System.out.println("done upload " + dirHash);
                    } finally {
                        transferQueueCounter.down("blob");
                    }
                    tm = System.currentTimeMillis() - tm;
                    log.info("Transferred blob in {} ms", tm);
                });
            }
        }

        @Override
        public byte[] getBlob(String hash) {
            byte[] arr = local.getBlob(hash);
            if (arr != null) {
                return arr;
            }
            return remote.getBlob(hash);
        }

        @Override
        public boolean hasBlob(String hash) {
            return remote.hasBlob(hash);
        }

        @Override
        public void checkComplete() throws InterruptedException {
            if (transferException != null) {
                throw new RuntimeException("Exception occured uploading blobs", transferException);
            }

            log.info("checkComplete transferJobs={} counter={}", transferJobs.size(), transferQueueCounter.count());
            while (transferQueueCounter.count() > 0 || !this.blobs.isEmpty()) {
                if (transferException != null) {
                    throw new RuntimeException("Exception occured uploading blobs", transferException);
                }
                log.info("..waiting for blob transfers to complete. transferJobs={} remaining={} blobs={}", transferJobs.size(), transferQueueCounter.count(), this.blobs.size());
                Thread.sleep(1000);
            }
            this.running = false;
        }

        public final void doBulkUpload(Set<BlobImpl> blobss) throws IOException {
            log.info("doBulkUpload: upload {} blobs", blobss.size());
            Path destPath = Path.path("/_hashes/blobs/").child("bulkBlobs.zip");
            transferQueueCounter.up(destPath.toString());
            transferExecutor.submit(() -> {
                try {
                    byte[] blobsZip;
                    try {
                        blobsZip = AppDeployerUtils.compressBulkBlobs(blobss);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }

                    HttpResult result;
                    try {
                        result = host.doPut(destPath, blobsZip, "application/zip");
                    } catch (Exception e) {
                        throw new RuntimeException("Error uploading zip - " + e.getMessage(), e);
                    }

                    if (result.getStatusCode() < 200 || result.getStatusCode() > 299) {
                        throw new RuntimeException("Failed to upload - " + result.getStatusCode());
                    }
                } catch (Exception ex) {
                    transferException = ex;
                    log.error("Exception in blobs transfer", ex);
                } finally {
                    transferQueueCounter.down(destPath.toString());
                    log.info("doBulkUpload: DONE upload {} blobs; transfer count={}", blobss.size(), transferQueueCounter.count());
                }
            });
        }
    }

    public static class FanoutBean {

        String hash;
        List<String> blobHashes;
        long actualContentLength;

        public FanoutBean(String hash, List<String> blobHashes, long actualContentLength) {
            this.hash = hash;
            this.blobHashes = blobHashes;
            this.actualContentLength = actualContentLength;
        }
    }

    public class AppDeployerHashStore implements BlockingHashStore {

        private final Host host;
        private final HashStore local;
        private final HashStore remote;

        private final BlockingQueue<FanoutBean> chunkBeans = new ArrayBlockingQueue<>(1000);
        private final BlockingQueue<FanoutBean> fileBeans = new ArrayBlockingQueue<>(1000);
        private final Counter transferQueueCounter = new Counter();

        private final Set<String> addedChunkFanouts = new HashSet<>();
        private final Set<String> addedFileFanouts = new HashSet<>();

        private boolean running = true;
        private Exception transferException;

        private final LinkedBlockingQueue<Runnable> transferJobs = new LinkedBlockingQueue<>(100);
        private final ThreadPoolExecutor.CallerRunsPolicy rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        private final ExecutorService transferExecutor = new ThreadPoolExecutor(5, 5, 60, TimeUnit.SECONDS, transferJobs, rejectedExecutionHandler);
        private final Thread queueProcessor;

        public AppDeployerHashStore(Host host, HashStore local, HashStore remote) {
            this.host = host;
            this.local = local;
            this.remote = remote;

            queueProcessor = new Thread(() -> {
                try {
                    while (running || !chunkBeans.isEmpty() || !fileBeans.isEmpty()) {

                        log.info("HashStore queue: chunks={} files={}", chunkBeans.size(), fileBeans.size());
                        boolean didNothing = true;

                        Set<FanoutBean> toUpload = new HashSet<>();
                        this.chunkBeans.drainTo(toUpload, 100);
                        if (!toUpload.isEmpty()) {
                            doBulkFanoutUpload(toUpload, true);
                            didNothing = false;
                        }

                        toUpload = new HashSet<>();
                        this.fileBeans.drainTo(toUpload, 100);
                        if (!toUpload.isEmpty()) {
                            doBulkFanoutUpload(toUpload, false);
                            didNothing = false;
                        }

                        if (didNothing) {
                            Thread.sleep(2000);
                        }
                    }
                    log.info("AppDeployerBlobStore: transfer process finished");
                } catch (Exception ex) {
                    this.transferException = ex;
                }
            });
            queueProcessor.start();
        }

        @Override
        public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
            if (addedChunkFanouts.contains(hash)) {
                return;
            }
            if (!running) {
                throw new RuntimeException("Store is already stopped");
            }
            addedChunkFanouts.add(hash);
            if (!local.hasChunk(hash)) {
                local.setChunkFanout(hash, blobHashes, actualContentLength);
            }
            if (remote.hasChunk(hash)) {
                //log.info("setChunkFanout: remote bloom filter says probably already has this chunk");
                return;
            }

            //chunkBeans.add(new FanoutBean(hash, blobHashes, actualContentLength));
            boolean didAdd = false;
            try {
                didAdd = chunkBeans.offer(new FanoutBean(hash, blobHashes, actualContentLength), 30l, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            if (!didAdd) {
                throw new RuntimeException("Queue is full and i waited AGES to add it");
            }
        }

        @Override
        public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
            //log.info("setFileFanout {}", hash);
            if (addedFileFanouts.contains(hash)) {
                return;
            }
            addedFileFanouts.add(hash);
            if (!local.hasFile(hash)) {
                local.setFileFanout(hash, fanoutHashes, actualContentLength);
            }

            boolean didAdd = false;
            try {
                didAdd = fileBeans.offer(new FanoutBean(hash, fanoutHashes, actualContentLength), 30l, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            if (!didAdd) {
                throw new RuntimeException("Queue is full and i waited AGES to add it");
            }
        }

        @Override
        public Fanout getFileFanout(String fileHash) {
            //log.info("getFileFanout {}", fileHash);
            return local.getFileFanout(fileHash);
        }

        @Override
        public Fanout getChunkFanout(String fanoutHash) {
            return local.getChunkFanout(fanoutHash);
        }

        @Override
        public boolean hasChunk(String fanoutHash) {
            return remote.hasChunk(fanoutHash);
        }

        @Override
        public boolean hasFile(String fileHash) {
            //log.info("hasFile {}", fileHash);
            return remote.hasFile(fileHash);
        }

        public final void doBulkFanoutUpload(Set<FanoutBean> toUpload, boolean isChunk) throws IOException {
            final Path destPath;
            if (isChunk) {
                destPath = Path.path("/_hashes/chunkFanouts/").child("fanouts.zip");
            } else {
                destPath = Path.path("/_hashes/fileFanouts/").child("fanouts.zip");
            }

            transferQueueCounter.up(destPath.toString());
            transferExecutor.submit(() -> {
                try {
                    byte[] chunksZip;
                    try {
                        chunksZip = AppDeployerUtils.compressBulkFanouts(toUpload);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }

                    HttpResult result;
                    try {
                        result = host.doPut(destPath, chunksZip, "application/zip");
                    } catch (Exception e) {
                        throw new RuntimeException("Error uploading zip - " + e.getMessage(), e);
                    }

                    if (result.getStatusCode() < 200 || result.getStatusCode() > 299) {
                        throw new RuntimeException("Failed to upload - " + result.getStatusCode());
                    } else {
                        log.info("doBulkFanoutUpload: upload status={} destPath={}", result.getStatusCode(), destPath);
                    }
                } catch (Exception ex) {
                    transferException = ex;
                    log.error("Exception in fanouts transfer", ex);
                } finally {
                    transferQueueCounter.down(destPath.toString());
                    log.info("doBulkUpload: DONE upload {} hashes; transfer count={}", toUpload.size(), transferQueueCounter.count());
                }
            });
            log.info("doBulkUpload: upload {} hashes; transfer count={}", toUpload.size(), transferQueueCounter.count());
        }

        @Override
        public void checkComplete() throws InterruptedException {
            if (transferException != null) {
                throw new RuntimeException("Fanouts transfer exception", transferException);
            }
            log.info("checkComplete transferJobs={} counter={}", transferJobs.size(), transferQueueCounter.count());
            while (transferQueueCounter.count() > 0 || !chunkBeans.isEmpty() || !fileBeans.isEmpty()) {
                if (transferException != null) {
                    throw new RuntimeException("Fanouts transfer exception", transferException);
                }
                log.info("..waiting for fanout transfers to complete. transfers in progress={} chunkQueue={} fileQueue={}", transferQueueCounter.count(), chunkBeans.size(), fileBeans.size());
                Thread.sleep(1000);
            }
            this.running = false;
        }
    }

    private class Counter {

        private List<String> list = new ArrayList<>();

        synchronized void up(String url) {
            list.add(url);
        }

        synchronized void down(String url) {
            list.remove(url);
        }

        public int count() {
            return list.size();
        }
    }

    public class Results {

        private List<String> infos = new ArrayList<>();
        private List<String> warnings = new ArrayList<>();
        private List<String> errors = new ArrayList<>();

        public List<String> getErrors() {
            return errors;
        }

        public List<String> getInfos() {
            return infos;
        }

        public List<String> getWarnings() {
            return warnings;
        }
    }

}
