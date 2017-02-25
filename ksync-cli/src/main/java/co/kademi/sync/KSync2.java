package co.kademi.sync;

import io.milton.event.EventManager;
import io.milton.event.EventManagerImpl;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.httpclient.Host;
import io.milton.httpclient.HttpException;
import io.milton.sync.HttpBlobStore;
import io.milton.sync.HttpBloomFilterHashCache;
import io.milton.sync.HttpHashStore;
import io.milton.sync.triplets.MemoryLocalTripletStore;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.store.FileSystem2BlobStore;
import org.hashsplit4j.store.FileSystem2HashStore;
import org.hashsplit4j.triplets.HashCalc;
import org.hashsplit4j.triplets.ITriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class KSync2 {

    private static final Logger log = LoggerFactory.getLogger(MemoryLocalTripletStore.class);

    public static void main(String[] arg) {
        List<String> list = new ArrayList<>();
        for (String s : arg) {
            list.add(s);
        }
        Properties props = new Properties();
        String userHome = System.getProperty("user.home");
        System.out.println("User home: " + userHome);
        File fUserHome = new File(userHome);
        File f = new File(fUserHome, ".ksync.properties");
        if (!f.exists()) {
            createBlankFile(f);
        } else {
            readProps(f, props);
        }

        String sLocalDir = get(list, "local", props);
        String sRemoteAddress = get(list, "remote", props);
        String user = get(list, "user", props);
        String pwd = get(list, "password", props);

        File localDir = new File(sLocalDir);
        if (!localDir.exists()) {
            System.out.println("The local sync dir does not exist: " + localDir.getAbsolutePath());
            return;
        }
        if (!localDir.isDirectory()) {
            System.out.println("The local sync path is not a directory: " + localDir.getAbsolutePath());
            return;
        }

        try {
            KSync2 sync = new KSync2(localDir, sRemoteAddress, user, pwd);
            sync.start();
        } catch (Exception ex) {
            System.out.println("Exception running monitor: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void createBlankFile(File f) {
        FileOutputStream fout;
        try {
            fout = new FileOutputStream(f);
            byte[] b;
            b = "".getBytes();
            fout.write(b);
            fout.close();
        } catch (Throwable e) {
            System.out.println("Couldnt create empty properties file, not big deal");
        } finally {

        }
    }

    public static void showUsage() {
        System.out.println("KSync dbFile localDir remoteAddress user password");
        System.out.println("    where..");
        System.out.println("    dbfile : directory to store sync status information. Usually in your home folder");
        System.out.println("    local : the directory to sync with the server");
        System.out.println("    remote : url to sync to. Eg http://admin.mydomain.olhub.com/websites/mysite/version1/");
        System.out.println("    user : user name to login to the server. Do not use your email address, use the userId (on your Kademi profile page)");
        System.out.println("    password : your Kademi password");
        System.out.println("");
        System.out.println("Example:");
        System.out.println("");
        System.out.println("java -jar ksync.jar ~/syncdb proj/my-site http://admin.myaccount.olhub.com/repositories/mysite/version1/ myloginname mypassword");
        System.out.println("");
        System.out.println("You can also place above named properties in ~/.ksync.properties and these will be used instead of command line arguments");
        System.out.println("For example, if you dont want your password in the command line, add a line like this to ~/.ksync.properties:");
        System.out.println("password=mypassword");

    }

    private static void readProps(File f, Properties props) {
        try (FileInputStream fi = new FileInputStream(f)) {
            props.load(fi);
        } catch (FileNotFoundException ex) {
            // meh
        } catch (IOException ex) {
            // meh
        }
    }

    private static String get(List<String> list, String name, Properties props) {
        String s;

        if (!list.isEmpty()) {
            s = list.get(0);
            list.remove(0);
        } else {
            s = props.getProperty(name);
        }

        if (s == null) {
            System.out.println("Couldnt find a value for property " + name + " either from the command line or from properties file. Showing usage:");
            System.out.println("...");
            showUsage();
            return null;
        }

        return s;
    }


    private final File localDir;
    private final EventManager eventManager;
    private final Host client;
    private final MemoryLocalTripletStore tripletStore;
    private final HttpBlobStore httpBlobStore;
    private final HttpHashStore httpHashStore;
    private final BlobStore localBlobStore;
    private final HashStore localHashStore;
    private final HashCalc hashCalc = HashCalc.getInstance();

    private KSync2(File localDir, String sRemoteAddress, String user, String pwd) throws MalformedURLException, IOException {
        this.localDir = localDir;
        eventManager = new EventManagerImpl();
        URL url = new URL(sRemoteAddress);
        client = new Host(url.getHost(), url.getPort(), user, pwd, null);
        client.setUseDigestForPreemptiveAuth(false);
        String branchPath = url.getFile();

        String tempDir = System.getProperty("java.io.tmpdir");
        File tmp = new File(tempDir);
        this.localBlobStore = new FileSystem2BlobStore(new File(tmp, "blobs"));
        this.localHashStore = new FileSystem2HashStore(new File(tmp, "hashes"));

        HttpBloomFilterHashCache blobsHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "blobs-bloom");
        HttpBloomFilterHashCache fileFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "files-bloom");
        HttpBloomFilterHashCache chunckFanoutHashCache = new HttpBloomFilterHashCache(client, branchPath, "type", "chunks-bloom");

        httpBlobStore = new HttpBlobStore(client, blobsHashCache);
        httpBlobStore.setBaseUrl("/_hashes/blobs/");

        httpHashStore = new HttpHashStore(client, chunckFanoutHashCache, fileFanoutHashCache);
        httpHashStore.setChunksBaseUrl("/_hashes/chunkFanouts/");
        httpHashStore.setFilesBasePath("/_hashes/fileFanouts/");

        tripletStore = new MemoryLocalTripletStore(localDir, eventManager, localBlobStore, localHashStore, (String rootHash) -> {
            try {
                log.info("File changed in {}, new repo hash {}", localDir, rootHash);
                push(client, rootHash, sRemoteAddress);

            } catch (Exception ex) {
                log.error("Exception in file changed event handler", ex);
            }
        }, null, null);
    }

    private void start() throws MalformedURLException, IOException {
        tripletStore.scan();
        tripletStore.start();
    }

    private void push(Host client, String rootHash, String sRemoteAddress) throws IOException {
        log.info("PUSH {}", rootHash);

        // walk the VFS and push hashes and blobs to the remote store. Anything
        // already in the remote store will be ignored
        walkVfs(rootHash, httpBlobStore, httpHashStore);


        // Now set the hash on the repo, and check for any missing objects
        Map<String, String> params = new HashMap<>();
        params.put("newHash", rootHash);
        params.put("validate", "true");
        try {
            String res = client.doPost(sRemoteAddress, params);
            log.info("Result", res);
            JSONObject jsonRes = JSONObject.fromObject(res);
            Object statusOb = jsonRes.get("status");
            if (statusOb != null) {
                Boolean st = (Boolean) statusOb;
                if (st) {
                    log.info("Completed ok");
                    return;
                }
            }
            // todo: check status
            Object dataOb = jsonRes.get("data");
            System.out.println("data: " + dataOb);
            JSONObject data = (JSONObject) dataOb;
            JSONArray missingChunksArr = (JSONArray) data.get("missingChunkFanouts");

            JSONArray missingBlobsArr = (JSONArray) data.get("missingBlobs");
            upload(missingBlobsArr, (String hash) -> {
                log.info("Upload missing blob {}", hash);
                byte[] arr = localBlobStore.getBlob(hash);
                httpBlobStore.setBlob(hash, arr);
            });

            upload(missingChunksArr, (String hash) -> {
                log.info("Upload missing chunk fanout {}", hash);
                Fanout fanout = localHashStore.getChunkFanout(hash);
                httpHashStore.setChunkFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
            });

            JSONArray missingFileFanoutsArr = (JSONArray) data.get("missingFileFanouts");
            upload(missingFileFanoutsArr, (String hash) -> {
                log.info("Upload missing file fanout {}", hash);
                Fanout fanout = localHashStore.getFileFanout(hash);
                httpHashStore.setFileFanout(hash, fanout.getHashes(), fanout.getActualContentLength());
            });

            push(client, rootHash, sRemoteAddress);

        } catch (HttpException | NotAuthorizedException | ConflictException | BadRequestException | NotFoundException ex) {
            log.error("Exception setting hash", ex);
        }
    }


    private void upload(JSONArray arr, Consumer<String> c) {
        for( Object o : arr) {
            String hash = o.toString();
            c.accept(hash);
        }
    }


    private void walkVfs(String dirHash, BlobStore httpBlobStore, HashStore httpHashStore) throws IOException {
        byte[] dirListBlob = localBlobStore.getBlob(dirHash);
        httpBlobStore.setBlob(dirHash, dirListBlob);

        List<ITriplet> triplets = hashCalc.parseTriplets(new ByteArrayInputStream(dirListBlob));
        for( ITriplet triplet :triplets) {
            if( triplet.getType().equals("d")) {
                walkVfs(triplet.getHash(), httpBlobStore, httpHashStore);
            } else {
                combine(triplet.getHash(), httpHashStore, httpBlobStore);
            }
        }
    }

    public void combine(String fileHash, HashStore hashStore, BlobStore blobStore) {
        Fanout fileFanout = localHashStore.getFileFanout(fileHash);

        for (String fanoutHash : fileFanout.getHashes()) {
            Fanout fanout = localHashStore.getChunkFanout(fanoutHash);
            List<String> hashes = fanout.getHashes();
            for (String hash : hashes) {
                if ( !blobStore.hasBlob(hash) ) {
                    byte[] arr = localBlobStore.getBlob(hash);
                    blobStore.setBlob(hash, arr);
                }
            }

            hashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
        }

        hashStore.setFileFanout(fileHash, fileFanout.getHashes(), fileFanout.getActualContentLength());
    }
}
