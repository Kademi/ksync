package co.kademi.sync;

import io.milton.sync.SyncCommand;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author brad
 */
public class KSync {

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

        String sDbFile = get(list, "dbfile", props);
        String sLocalDir = get(list, "local", props);
        String sRemoteAddress = get(list, "remote", props);
        String user = get(list, "user", props);
        String pwd = get(list, "password", props);
        try {
            SyncCommand.monitor(sDbFile, sLocalDir, sRemoteAddress, user, pwd);
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
}
