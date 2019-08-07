package co.kademi.sync;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Consumer;
import net.sf.json.JSONArray;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
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

    public static void withKSync(KSyncCommand c, CommandLine line, Options options, boolean backgroundSync) {
        KSyncUtils.withDir((File dir) -> {
            File configDir = new File(dir, ".ksync");
            Properties props = KSyncUtils.readProps(configDir);
            String url = props.getProperty("url");
            String user = props.getProperty("user");
            String sIgnore = props.getProperty("ignore");
            List<String> ignores = split(sIgnore);

            String pwd = line.getOptionValue("password");
            if (StringUtils.isBlank(pwd)) {
                Console con = System.console();
                if (con != null) {
                    char[] chars = con.readPassword("Enter your password for " + user + "@" + url + ":");
                    pwd = new String(chars);
                } else {
                    Scanner scanner = new Scanner(System.in);
                    System.out.println("Enter your password for " + user + "@" + url + ":");
                    pwd = scanner.next();
                }
            }

            try {
                Map<String, String> cookies = getCookies(props);
                KSync3 kSync3 = new KSync3(dir, url, user, pwd, configDir, backgroundSync, ignores, cookies);
                c.accept(configDir, kSync3);
            } catch (IOException ex) {
                System.out.println("Ex: " + ex.getMessage());
            }
        }, options);
    }

    private static List<String> split(String sIgnore) {
        List<String> list = new ArrayList<>();
        if (StringUtils.isNotBlank(sIgnore)) {
            for (String s : sIgnore.split(",")) {
                list.add(s.trim());
            }
        }
        return list;
    }

    public static Map<String, String> getCookies(File repoDir) {
        Properties props = readProps(repoDir);
        return getCookies(props);
    }

    public static Map<String, String> getCookies(Properties props) {
        Map<String, String> map = new HashMap<>();
        if (props.containsKey("userUrl")) {
            map.put("miltonUserUrl", props.getProperty("userUrl"));
        }
        if (props.containsKey("userUrlHash")) {
            map.put("miltonUserUrlHash", props.getProperty("userUrlHash"));
        }
        return map;
    }

    public interface KSyncCommand {

        void accept(File configDir, KSync3 k);
    }

    public static void writeProps(String url, String user, File repoDir) {
        Properties props = readProps(repoDir);
        props.put("url", url);
        props.put("user", user);
        writeProps(props, repoDir);
    }

    public static void writeProps(Properties props, File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        log.info("writeProps: updating file {}", file.getAbsolutePath());
        try (FileOutputStream fout = new FileOutputStream(file)) {
            props.store(fout, null);
        } catch (Throwable e) {
            System.out.println("Couldnt create props file: " + file.getAbsolutePath());
        }
    }

    public static void writeLoginProps(String userUrl, String userUrlHash, File repoDir) {
        repoDir.mkdirs();
        Properties props = readProps(repoDir);
        props.setProperty("userUrl", userUrl);
        props.setProperty("userUrlHash", userUrlHash);
        writeProps(props, repoDir);
    }

    public static Properties readProps(File repoDir) {
        File file = new File(repoDir, "ksync.properties");
        Properties props = new Properties();
        if (file.exists()) {
            try (FileInputStream fi = new FileInputStream(file)) {
                props.load(fi);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
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
        String s = props.getProperty("remoteHash");
        if (s.equals("null")) {
            return null;
        }
        return s;
    }

    public static void processHashes(JSONArray arr, Consumer<String> c) {
        for (Object o : arr) {
            String hash = o.toString();
            c.accept(hash);
        }
    }

}
