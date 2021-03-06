package co.kademi.sync;

import static co.kademi.deploy.AppDeployer.DEFAULT_VERSION;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author brad
 */
public class KSync3Utils {

    public static KSync3.Command findCommand(CommandLine line, List<KSync3.Command> commands) {
        String cmd = line.getOptionValue("command");
        for (KSync3.Command c : commands) {
            if (c.getName().equals(cmd)) {
                return c;
            }
        }
        return null;
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

    public static String getInput(Options options, CommandLine line, String optionName, Properties props) {
        return getInput(options, line, optionName, props, true);
    }

    public static String getInput(Options options, CommandLine line, String optionName, Properties props, boolean promptIfNotPresent) {
        if (props != null && props.containsKey(optionName)) {
            String s = props.getProperty(optionName);
            if (StringUtils.isNotBlank(s)) {
                return s;
            }
        }

        if (!promptIfNotPresent) {
            return null;
        }
        String s = line.getOptionValue(optionName);
        if (StringUtils.isBlank(s)) {
            Option opt = options.getOption(optionName);
            Console con = System.console();
            if (con != null) {
                s = con.readLine("Please enter " + optionName + " - " + opt.getDescription() + ": ");
            } else {
                Scanner scanner = new Scanner(System.in);
                System.out.println("Please enter " + optionName + " - " + opt.getDescription() + ": ");
                s = scanner.nextLine();
            }
        }
        return s;
    }

    public static String getPassword(CommandLine line, String user, String url) {
        String s = line.getOptionValue("password");
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

    public static File getRootDir(CommandLine line) {
        String s = line.getOptionValue("rootdir");
        if (StringUtils.isBlank(s)) {
            s = System.getProperty("user.dir");
        }
        //s = "/home/brad/proj/kademi-dev/src/main/marketplace/";
        return new File(s);
    }

    public static boolean getBooleanInput(CommandLine line, String opt) {
        return line.hasOption(opt);
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

    static boolean ignored(String name, List<String> ignores) {
        if (ignores == null) {
            return false;
        }
        for (String s : ignores) {
            if (name.equals(s)) {
                return true;
            }
        }
        return false;
    }

}
