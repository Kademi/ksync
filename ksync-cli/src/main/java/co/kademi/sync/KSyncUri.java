package co.kademi.sync;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * This class is for accepting ksync:// uri schema and call the ksync3 to execute commands
 * @author kademi
 */
public class KSyncUri {
    
    private static boolean isUriSchema = false;
    private static File appDir;
    private static boolean checkout = false;
    
    /**
     * this method is to check if the provided arg is an URI schema
     * @param args
     * @return 
     */
    public static boolean isUri(String[] args) {
        String URI = args[0];
        KSyncUri.setIsUriSchema(true);
        return URI.startsWith("ksync://");
    }
    
    public static String[] getURIArguments(String[] args) {
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
        String appFolder = "";
        String[] arrCommands = strCommands.split(" ");
        ArrayList<String> listArgs = new ArrayList();
        int i = 0;
        int temp = -1;
        for (String arrCommand : arrCommands) {
            if (arrCommand != null && arrCommand.trim().length() > 1) {
                //System.out.println(arrCommand);
                
                if(arrCommand.equals("checkout")) {
                    System.out.println("Ksync3: checkout command found");
                    createAppFolder(arrCommands[3]);
                    KSyncUri.setCheckout(true);
                }
                
                if(arrCommand.equals("-app")) {
                    temp = i + 1;
                    appFolder = arrCommands[temp];
                } else if(i != temp){
                    listArgs.add(arrCommand);
                }
            }
            i++;
        }
        
        if(StringUtils.isNotEmpty(appFolder)) {
            String path = System.getProperty("user.home") +"/"+ appFolder;
            File file = new File(path);
            if(!file.exists()) {
                System.out.println("KSync3: Cannot find app folder in user home directory. path="+file.getAbsolutePath());
                System.exit(0);
            }
            KSyncUri.setAppDir(file);
        }
        
        if(!KSyncUri.isCheckout() && StringUtils.isEmpty(appFolder)) {
            System.out.println("KSync3: For uri schema, -app argument is required.");
            System.exit(0);
        }
        
        String[] arguments = new String[listArgs.size()];
        arguments = listArgs.toArray(arguments);
                
        return arguments;
    }
    
    private static void createAppFolder(String strUrl) {
        System.out.println("Ksync3: parsing app name from url: "+strUrl);
        try {
            
            URL url = new URL(strUrl);
            url.toURI();
            String[] urlParts = strUrl.split("/");
            String appName = urlParts[4].trim();
            if(appName.length() == 0) {
                System.out.println("KSync3: App name cannot be empty: URL: "+strUrl+", appName: "+appName);
                System.exit(0);
            }
            
            String path = System.getProperty("user.home") +"/"+ appName;
            System.out.println("KSync3: check path: "+path);
            File file = new File(path);
            if(!file.exists()) {
                System.out.println("KSync3: "+appName+" not exist in user home directory. creating folder");
                file.mkdir();
            }
            KSyncUri.setAppDir(file);
            
        } catch (MalformedURLException ex) {
            System.out.println("KSync3: This is invalid checkout url at I. message: "+ex.getMessage());
        } catch (URISyntaxException ex) {
            System.out.println("KSync3: This is invalid checkout url at II. message: "+ex.getMessage());
        }
    }

    public static boolean isIsUriSchema() {
        return isUriSchema;
    }

    public static void setIsUriSchema(boolean isUriSchema) {
        KSyncUri.isUriSchema = isUriSchema;
    }

    public static File getAppDir() {
        return appDir;
    }

    public static void setAppDir(File appDir) {
        KSyncUri.appDir = appDir;
    }

    public static boolean isCheckout() {
        return checkout;
    }

    public static void setCheckout(boolean checkout) {
        KSyncUri.checkout = checkout;
    }

    
}