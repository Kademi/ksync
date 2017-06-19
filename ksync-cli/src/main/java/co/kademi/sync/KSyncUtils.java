package co.kademi.sync;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Consumer;
import net.sf.json.JSONArray;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class KSyncUtils {

    private static final Logger log = LoggerFactory.getLogger(KSyncUtils.class);

    public static void withDir(Consumer<File> s, Options options) {
        String curDir = System.getProperty("user.dir");
        File dir = new File(curDir);

        if (!dir.exists()) {
            log.error("Directory does not exist: " + dir.getAbsolutePath());
        } else {
            if (!dir.isDirectory()) {
                log.error("Is not a directory: " + dir.getAbsolutePath());
            } else {
                s.accept(dir);
            }
        }
    }

    public static void withKSync(KSyncCommand c, Options options) {
        KSyncUtils.withDir((File dir) -> {
            File configDir = new File(dir, ".ksync");
            Properties props = KSyncUtils.readProps(configDir);
            String url = props.getProperty("url");
            String user = props.getProperty("user");

            Console con = System.console();
            String pwd;
            if (con != null) {
                char[] chars = con.readPassword("Enter your password for " + user + "@" + url + ":");
                pwd = new String(chars);
            } else {
                Scanner scanner = new Scanner(System.in);
                System.out.println("Enter your password for " + user + "@" + url + ":");
                pwd = scanner.next();
            }

            try {
                KSync3 kSync3 = new KSync3(dir, url, user, pwd, configDir);
                c.accept(configDir, kSync3);
            } catch (IOException ex) {
                System.out.println("Ex: " + ex.getMessage());
            }
        }, options);
    }

    public interface KSyncCommand {

        void accept(File configDir, KSync3 k);
    }

    public static void writeProps(String url, String user, File repoDir) {
        Properties props = new Properties();
        props.put("url", url);
        props.put("user", user);
        writeProps(props, repoDir);
    }

    public static void writeProps(Properties props, File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        try (FileOutputStream fout = new FileOutputStream(file)) {
            props.store(fout, null);
        } catch (Throwable e) {
            System.out.println("Couldnt create props file: " + file.getAbsolutePath());
        }
    }

    public static Properties readProps(File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        Properties props = new Properties();
        try (FileInputStream fi = new FileInputStream(file)) {
            props.load(fi);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return props;
    }

    public static void saveRemoteHash(File repoDir, String rootHash) {
        Properties props = readProps(repoDir);
        props.setProperty("remoteHash", rootHash);
        writeProps(props, repoDir);
    }

    public static String getLastRemoteHash(File repoDir) {
        Properties props = readProps(repoDir);
        return props.getProperty("remoteHash");

    }

    public static void processHashes(JSONArray arr, Consumer<String> c) {
        for (Object o : arr) {
            String hash = o.toString();
            c.accept(hash);
        }
    }


}
