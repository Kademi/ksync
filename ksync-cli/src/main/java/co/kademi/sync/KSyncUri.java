package co.kademi.sync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;

/**
 * This class is for accepting ksync:// uri schema and call the ksync3 to execute commands
 * @author kademi
 */
public class KSyncUri {
    
    public static void main(String[] args) throws IOException {
        // Check how many arguments were passed in
        if(args == null || args.length == 0)
        {
            System.out.println("Cannot find any arguments. Usage: ksync://<encoded string>");
            System.exit(0);
        }
        
        String URI = args[0];
        if(!URI.contains("ksync://")) {
            System.out.println("Invalid argument. Usage: ksync://<encoded string>");
            System.exit(0);
        }
        //ksync://LWNvbW1hbmQgc3luYw==
        String encoded = URI.replace("ksync://", "");
        
        //decoding base64
        Base64.Decoder decoder = Base64.getDecoder();  
        String strCommands = new String(decoder.decode(encoded));  
        
        
        //parsing decoded string to array of args
        String[] arrCommands = strCommands.split(" ");
        ArrayList<String> listArgs = new ArrayList();
        for (String arrCommand : arrCommands) {
            if (arrCommand != null && arrCommand.trim().length() > 1) {
                listArgs.add(arrCommand);
            }
        }
        
        String[] arguments = new String[listArgs.size()];
        arguments = listArgs.toArray(arguments);
                
        //calling the ksync3 main class
        KSync3.main(arguments);
    }
}