package co.kademi.sync;

import java.io.Console;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class KSync3Utils {

    private static final Logger log = LoggerFactory.getLogger(KSync3Utils.class);

    public static String makeFileName(String url) {
        String fname = url.replace("/", "-");
        fname = fname.replace("https", "");
        fname = fname.replace("http", "");
        fname = fname.replace(":8080", "");
        fname = fname.replace(":80", "");
        fname = fname.replace(":", "");
        return fname;
    }

    public static String getInput(String text) {
        Console con = System.console();
        String s;
        if (con != null) {
            s = con.readLine("Please enter " + text + ": ");
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please enter " + text + ": ");
            s = scanner.nextLine();
        }
        return s;
    }

    /**
     * The value to use for an option, asking for it when it is missing and
     * there is someone to ask.
     *
     * The command line is the first source, and picocli has already filled in
     * anything the checkout's own properties file could answer, so what reaches
     * here is genuinely absent.
     *
     * @param given
     * @param optionName
     * @param description
     * @return
     */
    public static String resolve(String given, String optionName, String description) {
        return resolve(given, optionName, description, true);
    }

    public static String resolve(String given, String optionName, String description, boolean promptIfNotPresent) {
        if (StringUtils.isNotBlank(given)) {
            return given;
        }
        if (!promptIfNotPresent) {
            return null;
        }
        Console con = System.console();
        if (con != null) {
            return con.readLine("Please enter " + optionName + " - " + description + ": ");
        }
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter " + optionName + " - " + description + ": ");
        return scanner.nextLine();
    }

    public static String getPassword(String given, String user, String url) {
        String s = given;
        if (StringUtils.isBlank(s)) {
            Console con = System.console();
            if (con != null) {
                char[] chars = con.readPassword("Enter your password for " + user + "@" + url + ": ");
                s = new String(chars);
            } else {
                Scanner scanner = new Scanner(System.in);
                System.out.println("Enter your password for " + user + "@" + url + ": ");
                s = scanner.next();
            }
        }
        return s;
    }

    /**
     * The directory a command works on, without creating anything.
     *
     * Read only because the default value provider calls it while the command
     * line is still being parsed, and parsing must not leave directories behind
     * on disk.
     *
     * @param appDir
     * @param appName
     * @return
     */
    public static File checkoutDir(String appDir, String appName) {
        String curDir = StringUtils.isNotEmpty(appDir) ? appDir : System.getProperty("user.dir");
        if (StringUtils.isNotEmpty(appName)) {
            curDir = curDir + "/" + appName;
        }
        return new File(curDir);
    }

    /**
     * The same directory, created if a ksync:// link named one that does not
     * exist yet.
     *
     * @param appDir
     * @param appName
     * @return
     */
    public static String getOrCreateAppDirectory(String appDir, String appName) {
        if (StringUtils.isNotEmpty(appDir) && StringUtils.isEmpty(appName)) {
            log.error("The appname option is required when using a ksync uri");
            System.exit(1);
        }
        File dir = checkoutDir(appDir, appName);
        if (StringUtils.isNotEmpty(appName) && !dir.exists()) {
            log.info("Creating the app directory {}", dir.getAbsolutePath());
            dir.mkdir();
        }
        return dir.getAbsolutePath();
    }

    public static List<String> split(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        } else {
            String[] arr = s.split(",");
            List<String> list = new ArrayList<>();
            for (String ss : arr) {
                ss = ss.trim();
                list.add(ss);
            }
            return list;
        }
    }

}
