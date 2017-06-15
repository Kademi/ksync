/*
 *       Copyright FuseLMS
 */
package co.kademi.deploy;

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

    private static String readLine(Console con, Scanner scanner, String msg) {
        System.out.println(msg + ": ");
        if (con != null) {
            return con.readLine();
        }
        return scanner.next();
    }

    private static String readPassword(Console con, Scanner scanner, String msg) {
        System.out.println(msg + ": ");
        if (con != null) {
            char[] cars = con.readPassword();
            return new String(cars);
        }
        return scanner.next();
    }

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

    public static void main(String[] args) throws MalformedURLException, IOException {
        String userDir = System.getProperty("user.dir");
        File dir = new File(userDir);
        if (!dir.exists()) {
            System.out.println("Dir not found: " + dir.getAbsolutePath());
            return;
        }
        log.info("Current directory: " + dir.getAbsolutePath());
        String url;
        String user;
        String password;
        String appIds;

        if (args.length >= 4) {
            url = args[0];
            user = args[1];
            password = args[2];
            appIds = args[3];
        } else {
            Console con = System.console();
            Scanner scanner = null;
            if (con == null) {
                scanner = new Scanner(System.in);
            }
            url = readLine(con, scanner, "Please enter a host url, eg http://localhost:8080/");
            user = readLine(con, scanner, "Please enter your userid (not email)");
            password = readPassword(con, scanner, "Please enter your password");

            appIds = readLine(con, scanner, "* to load all apps, or enter a comma seperated list of ids or absolute paths, eg /libs");
            if (appIds.equals("*")) {
                appIds = "";
            }

            System.out.println("Cheers..");
        }
        AppDeployer d = new AppDeployer(dir, url, user, password, appIds);

        d.upsync();

        log.info("Completed");
        System.exit(0);
    }

    private final File rootDir;
    private final Host client;
    private final List<String> appIds;

    private HttpBlobStore httpBlobStore;
    private HttpHashStore httpHashStore;

    private final BlobStore localBlobStore;
    private final HashStore localHashStore;

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

    public void upsync() throws IOException {
        upSyncMarketplaceDir(new File(rootDir, "themes"), true, false);
        upSyncMarketplaceDir(new File(rootDir, "apps"), false, false);
        upSyncMarketplaceDir(new File(rootDir, "libs"), false, true);

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

                    log.info("checkCreateApp {} {}", appName);
                    String appPath = "/manageApps/" + appName;
                    boolean appCreated = false;
                    if (!doesExist(appPath)) {
                        if (createApp(appName, isTheme, isApp)) {
                            appCreated = true;
                            log.info("created app {}", appName);
                        } else {
                            throw new RuntimeException("Couldnt create app " + appName);
                        }
                    }

//                try {
//                    createVersion(" /repositories/d3-lib/3.4.1 -> 1.0.0", "1.0.0");
//                } catch (Exception e) {
//                    System.out.println("eek");
//                }
                    String versionName = AppDeployer.findVersion(appDir);
                    upSyncMarketplaceVersionDir(appName, versionName, isTheme, isApp, appDir);

                    if (appCreated) {
                        if (!addToMarketPlace(appName)) {
                            throw new RuntimeException("Failed to add app to marketplace " + appPath);
                        }
                    }
                    if (!publishApp(appName)) {
                        throw new RuntimeException("Failed to publish app to marketplace " + appPath);
                    }
                }
            }
        }
    }

    private void upSyncMarketplaceVersionDir(String appName, String versionName, boolean theme, boolean app, File localRootDir) {
        try {
            checkCreateAppVersion(appName, versionName, theme, app);

            String branchPath = "/repositories/" + appName + "/" + versionName + "/";

            HttpBloomFilterHashCache blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
            HttpBloomFilterHashCache chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");
            HttpBloomFilterHashCache fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");

            httpBlobStore = new HttpBlobStore(client, blobsHashCache);
            httpBlobStore.setBaseUrl("/_hashes/blobs/");

            httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
            httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
            httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

            MinimalPutsBlobStore mpBlobStore = new MinimalPutsBlobStore(httpBlobStore);
            MinimalPutsHashStore mpHashStore = new MinimalPutsHashStore(httpHashStore);

            AppDeployerBlobStore blobStore = new AppDeployerBlobStore(localBlobStore, mpBlobStore);
            AppDeployerHashStore hashStore = new AppDeployerHashStore(localHashStore, mpHashStore);

            MemoryLocalTripletStore s = new MemoryLocalTripletStore(localRootDir, new EventManagerImpl(), blobStore, hashStore, (String rootHash) -> {
                try {
                    log.info("File changed in {}, new repo hash {}", localRootDir, rootHash);
                    push(rootHash, branchPath);
                } catch (Exception ex) {
                    log.error("Exception in file changed event handler", ex);
                }
            }, null, null);
            s.scan();
        } catch (Exception ex) {
            log.error("Exception upsyncing " + appName, ex);
        }
    }

    private void push(String localRootHash, String branchPath) throws IOException, InterruptedException {
        String remoteHash = getRemoteHash(branchPath);
        if (remoteHash == null) {
            log.info("Aborted");
            return;
        }
        if (remoteHash.equals(localRootHash)) {
            log.info("No change. Local repo is exactly the same as remote hash={}", localRootHash);
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
            log.info("Push failed: But missing objects have been uploaded so please try again :)", res);

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            log.error("Exception setting hash", ex);
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
        // always publish new versions
        publishVersion(appBasPath, versionName);

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
            if (versions.isEmpty()) {
                throw new RuntimeException("Cannot create a version because there is no initial version: " + appBasPath);
            }
            versions.sort(ComparatorUtils.NATURAL_COMPARATOR);
            String highestVersion = versions.get(versions.size() - 1);
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

    /**
     * Make this the live branch
     *
     * @param appBasPath
     * @param versionName
     * @return
     */
    private boolean publishVersion(String appBasPath, String versionName) {
        log.info("publishVersion: app path={} version={}", appBasPath, versionName);
        // http://localhost:8080/repositories/test1/1.0.0/publish
        String pubPath = appBasPath + versionName + "/publish";
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

    private boolean addToMarketPlace(String appName) {
        log.info("addToMarketPlace: {}", appName);
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
            log.info("add to market place failed", res);
            return false;

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            throw new RuntimeException("Exception publishing app to marketplace" + appName, ex);
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
            if (s.equals(appDir.getName())) {
                return true;
            }
        }
        return false;
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
