package co.kademi.sync;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;

/**
 * This class is for accepting ksync:// uri schema and call the ksync3 to execute commands
 * @author kademi
 */
public class KSyncUri {
    
    /**
     * this method is to check if the provided arg is an URI schema
     * @param args
     * @return 
     */
    public static boolean isUri(String[] args) {
        String arg = args[0];
        return arg.startsWith("ksync://");
    } 
    
    public static String[] parseArguments(String[] args) {
        
        
        String URI = args[0];
        if(!URI.contains("ksync://")) {
            System.out.println("Invalid argument. Usage: ksync://<encoded string>");
            System.exit(0);
        }
        
        //ksync://LWNvbW1hbmQgc3luYw==
        String encoded = URI.replace("ksync://", "");
        String strCommands = "";
        byte[] decoded = Base64.decodeBase64(encoded);
        try{
            strCommands = new String(decoded, "UTF-8");
        }catch(UnsupportedEncodingException e) {
            System.out.println("Decode error" + e.getMessage());
        }
        
        //parsing decoded string to array of args
        boolean hasAppDirOption = false;
        String[] arrCommands = strCommands.split(" ");
        ArrayList<String> listArgs = new ArrayList();
        for (String arrCommand : arrCommands) {
            if (arrCommand != null && arrCommand.trim().length() > 1) {
                if(arrCommand.equals("-appdir")) {
                    hasAppDirOption = true;
                }
                listArgs.add(arrCommand);
            }
        }
        
        if(!hasAppDirOption) {
            listArgs.add("-appdir");
            listArgs.add(System.getProperty("user.home"));
        }
        
        String[] arguments = new String[listArgs.size()];
        arguments = listArgs.toArray(arguments);
                
        return arguments;
    }    
}