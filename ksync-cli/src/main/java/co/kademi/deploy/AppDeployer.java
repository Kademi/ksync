/*
 *       Copyright FuseLMS
 */
package co.kademi.deploy;

import co.kademi.sync.KSync3Utils;
import io.milton.common.Path;
import io.milton.event.EventManagerImpl;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import io.milton.httpclient.PropFindResponse;
import io.milton.httpclient.RespUtils;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.MinimalPutsBlobStore;
import io.milton.sync.MinimalPutsHashStore;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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

    public static void publish(Options options, CommandLine line) throws MalformedURLException, IOException {
        File dir = KSync3Utils.getRootDir(line);
        //userDir = "/home/brad/proj/kademi-dev/src/main/marketplace/";

        if (!dir.exists()) {
            System.out.println("Dir not found: " + dir.getAbsolutePath());
            return;
        }
        log.info("Current directory: " + dir.getAbsolutePath());
        String url = KSync3Utils.getInput(options, line, "url", null);
        String user = KSync3Utils.getInput(options, line, "user", null);
        String password = KSync3Utils.getPassword(line, user, url);
        String appIds = KSync3Utils.getInput(options, line, "appids", null);

        AppDeployer d = new AppDeployer(dir, url, user, password, appIds);
        d.autoIncrement = KSync3Utils.getBooleanInput(line, "versionincrement");
        d.force = KSync3Utils.getBooleanInput(line, "force");
        d.report = KSync3Utils.getBooleanInput(line, "report");

        log.info("---- OPTIONS ----");
        log.info("url: " + url);
        log.info("dir: " + dir.getAbsolutePath());
        log.info("  -autoincrement: " + d.autoIncrement);
        log.info("  -report: " + d.report);
        log.info("  -force: " + d.force);
        log.info("--------------");
        d.upsync();

        log.info("Completed");
        System.exit(0);
    }

    private final File rootDir;
    private final Host client;
    private final List<String> appIds;
    private final List<String> results = new ArrayList<>();

    private HttpBlobStore httpBlobStore;
    private HttpHashStore httpHashStore;

    private final BlobStore localBlobStore;
    private final HashStore localHashStore;

    private boolean autoIncrement = false; // if true, update version numbers in files
    private boolean report; // if true, dont make any changes
    private boolean force;

    public AppDeployer(File dir, String sRemoteAddress, String user, String password, String sAppIds) throws MalformedURLException {
        this.rootDir = dir;

        URL url = new URL(sRemoteAddress);
        client = new Host(url.getHost(), url.getPort(), user, password, null);
        client.setTimeout(30000);
        client.setUseDigestForPreemptiveAuth(false);
        String s = sAppIds;
        if (StringUtils.isBlank(s)) {
            this.appIds = null;
        } else {
            this.appIds = new ArrayList<>();
            for (String ss : s.split(",")) {
                ss = ss.trim();
                appIds.add(ss);
            }
        }

        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File localDataDir = new File(tmpDir, "appDeployer");
        log.info("Using local data dir {}", localDataDir);

        localBlobStore = new FileSystem2BlobStore(new File(localDataDir, "blobs"));
        localHashStore = new FileSystem2HashStore(new File(localDataDir, "hash"));
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void upsync() throws IOException {
        upSyncMarketplaceDir(new File(rootDir, "themes"), true, false);
        upSyncMarketplaceDir(new File(rootDir, "apps"), false, false);
        upSyncMarketplaceDir(new File(rootDir, "libs"), false, true);

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("-------- RESULTS -------------");
        for (String s : results) {
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
        for (File appDir : dir.listFiles()) {
            if (appDir.isDirectory()) {
                String appName = appDir.getName();

                if (isProcess(appDir)) {
                    String result = processAppDir(appName, isTheme, isApp, appDir);
                    results.add(result);
                }
            }
        }
    }

    private String processAppDir(String appName, boolean isTheme, boolean isApp, File appDir) throws RuntimeException {
        log.info("checkCreateApp {} {}", appName);
        String appPath = "/manageApps/" + appName;
        boolean appCreated = false;
        if (!doesExist(appPath)) {
            if (createApp(appName, isTheme, isApp)) {
                appCreated = true;
                log.info("created app {}", appName);
            } else {
                return "Couldnt create app " + appName;
            }
        }

        String versionName = AppDeployer.findVersion(appDir);
        String appVersionPath = "/repositories/" + appName + "/" + versionName + "/";
        boolean doesVersionExist = doesExist(appVersionPath);

        if (doesVersionExist) {
            log.info("App version is already published " + appVersionPath);

            // Check that local and remote hashes match, and warn if they dont
            String localHash = getLocalHash(appName, versionName, appDir);
            if (localHash == null) {
                log.warn("Could not get local hash for " + appDir);
                if (!force) {
                    return appName + " version " + versionName + " is already published, but could not find local hash to compare";
                }
            } else {
                String branchPath = "/repositories/" + appName + "/" + versionName + "/";
                String remoteHash = getRemoteHash(branchPath);
                if (localHash.equals(remoteHash)) {
                    if (force) {
                        log.info("App is an exact match local and remote ={} but force is true, so push anyway", localHash);
                    } else {
                        log.info("App is an exact match local and remote ={}", localHash);
                        return appName + " version " + versionName + " is already published, and exactly matches local " + localHash;
                    }
                } else {
                    if (force) {
                        log.warn("App is not the same as published version, but force is true so will republish. app={} version={} local={} remote={}", appName, versionName, localHash, remoteHash);
                    } else {
                        log.warn("App is not the same as published version. app={} version={} local={} remote={}", appName, versionName, localHash, remoteHash);
                        return "WARN " + appName + " version " + versionName + " is already published, but differs from local=" + localHash + " remote=" + remoteHash;
                    }
                }
            }
        }

        String localHash = upSyncMarketplaceVersionDir(appName, versionName, isTheme, isApp, appDir);
        if (localHash != null) {

            if (appCreated) {
                if (!addToMarketPlace(appName)) {
                    return "Did not add app to marketplace " + appPath;
                }
            }

            String branchPath = "/repositories/" + appName + "/";

            if (report) {
                return "Would have published version " + branchPath + "/" + versionName + " with local hash " + localHash;
            } else {
                if (makeCurrentVersionLive(branchPath, versionName)) {
                    // Republic, so the live version is the published version
                    if (!publishApp(appName)) {
                        return "Pushed, but could not (re)publish app to marketplace " + appPath;
                    }
                    
                    if (autoIncrement) {
                        if (report) {
                            log.info("Would have auto-incremented " + appDir);
                        } else {                            
                            incrementVersionNumber(appDir, versionName);
                        }
                    }

                    return "Published " + appName + " version " + versionName + " with hash " + localHash;
                } else {
                    return "Failed to publish version " + appName;
                }
            }

        } else {
            return "Failed to sync local to remote " + appName;
        }

    }

    private String upSyncMarketplaceVersionDir(String appName, String versionName, boolean theme, boolean app, File localRootDir) {
        log.info("upSyncMarketplaceVersionDir app={}", appName);
        try {
            checkCreateAppVersion(appName, versionName, theme, app);

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
                blobStore = new AppDeployerBlobStore(localBlobStore, mpBlobStore);
                hashStore = new AppDeployerHashStore(localHashStore, mpHashStore);
            }

            MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), blobStore, hashStore, (String rootHash) -> {
                try {
                    log.info("File changed in {}, new repo hash {}", localRootDir, rootHash);
                    push(rootHash, branchPath);

                } catch (Exception ex) {
                    log.error("Exception in file changed event handler", ex);
                }
            }, null, null);
            return s.scan();
        } catch (Exception ex) {
            log.error("Exception upsyncing " + appName, ex);
            return null;
        }
    }

    private void push(String localRootHash, String branchPath) {
        String remoteHash = getRemoteHash(branchPath);
        if (remoteHash == null) {
            log.info("Aborted");
            return;
        }
        if (remoteHash.equals(localRootHash)) {
            log.info("No change. Local repo is exactly the same as remote hash={}", localRootHash);
            return;
        }
        if (report) {
            log.info("Not doing push {} because in report mode", branchPath);
            return;
        }

        // Now set the hash on the repo, and check for any missing objects
        Map<String, String> params = new HashMap<>();
        params.put("newHash", localRootHash);
        params.put("validate", "true");
        try {
            log.info("PUSH Local: {} Remote: {}", localRootHash, remoteHash);
            String res = client.post(branchPath, params);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Completed ok");
                    return;
                }
            }
            System.out.println("jsonRes " + jsonRes);
            JSONObject data = (JSONObject) jsonRes.get("data");
            if (data != null) {
                httpHashStore.setForce(true);
                httpBlobStore.setForce(true);
                JSONArray missing = (JSONArray) data.get("missingFileFanouts");
                for (Object ff : missing.toArray()) {
                    log.info("Missing file fanout: {}", ff);
                    String missingHash = ff.toString();
                    Fanout f = localHashStore.getFileFanout(missingHash);
                    if (f == null) {
                        log.error("Could not find locally missing file fanout: " + missingHash);
                        return;
                    }
                    httpHashStore.setFileFanout(missingHash, f.getHashes(), f.getActualContentLength());
                    log.info("Uploaded missing file fanout");
                }

                // missingChunkFanouts
                missing = (JSONArray) data.get("missingChunkFanouts");
                for (Object ff : missing.toArray()) {
                    log.info("Missing chunk fanout: {}", ff);
                    String missingHash = ff.toString();
                    Fanout f = localHashStore.getChunkFanout(missingHash);
                    if (f == null) {
                        log.error("Could not find locally missing chunk fanout: " + missingHash);
                        return;
                    }
                    httpHashStore.setChunkFanout(missingHash, f.getHashes(), f.getActualContentLength());
                    log.info("Uploaded missing chunk fanout");
                }

                // missingBlobs
                missing = (JSONArray) data.get("missingBlobs");
                for (Object ff : missing.toArray()) {
                    log.info("Missing blob: {}", ff);
                    String missingHash = ff.toString();
                    byte[] f = localBlobStore.getBlob(missingHash);
                    if (f == null) {
                        log.error("Could not find locally missing blob: " + missingHash);
                        return;
                    }
                    httpBlobStore.setBlob(missingHash, f);
                    log.info("Uploaded missing blob");
                }

            }
            log.info("Push failed: But missing objects have been uploaded so will try again :)", res);
            push(localRootHash, branchPath);

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            results.add("EXCEPTION: Failed to push to " + branchPath + " because " + ex.getMessage());
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

    private void checkCreateAppVersion(String appName, String versionName, boolean theme, boolean app) throws NotAuthorizedException, UnknownHostException, SocketTimeoutException, IOException, ConnectException, HttpException {

        // Now check for the version
        String appBasPath = "/repositories/" + appName + "/";
        String versionPath = appBasPath + versionName;
        if (!doesExist(versionPath)) {
            log.info("Version does not exist app={} version={}", appName, versionName);
            if (createVersion(appBasPath, versionName)) {
                log.info("Created version {}", versionName);
            } else {
                throw new RuntimeException("Couldnt create version " + versionPath);
            }
        } else {
            log.info("Version already exists app={} version={}", appName, versionName);
        }

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
            List<PropFindResponse> list = client.propFind(p, 1, RespUtils.davName("name"), RespUtils.davName("resourcetype"), RespUtils.davName("iscollection"));
            List<String> versions = new ArrayList<>();
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

            httpBlobStore = new HttpBlobStore(client, blobsHashCache);
            httpBlobStore.setBaseUrl("/_hashes/blobs/");

            httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
            httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
            httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

            MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), localBlobStore, localHashStore, (String rootHash) -> {
            }, null, null);
            return s.scan();
        } catch (Exception ex) {
            log.error("Could not find local hash for " + localRootDir, ex);
            return null;
        }
    }

    public class AppDeployerBlobStore implements BlobStore {

        private final BlobStore local;
        private final BlobStore remote;

        public AppDeployerBlobStore(BlobStore local, BlobStore remote) {
            this.local = local;
            this.remote = remote;
        }

        @Override
        public void setBlob(String hash, byte[] bytes) {
            local.setBlob(hash, bytes);
            remote.setBlob(hash, bytes);
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
    }

    public class AppDeployerHashStore implements HashStore {

        private final HashStore local;
        private final HashStore remote;

        public AppDeployerHashStore(HashStore local, HashStore remote) {
            this.local = local;
            this.remote = remote;
        }

        @Override
        public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
            local.setChunkFanout(hash, blobHashes, actualContentLength);
            remote.setChunkFanout(hash, blobHashes, actualContentLength);
        }

        @Override
        public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
            //log.info("setFileFanout {}", hash);
            local.setFileFanout(hash, fanoutHashes, actualContentLength);
            remote.setFileFanout(hash, fanoutHashes, actualContentLength);
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

    }
}
