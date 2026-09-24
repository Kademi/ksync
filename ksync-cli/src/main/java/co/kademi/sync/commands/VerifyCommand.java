package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "verify",
        header = "Find, and send, content missing from the server.",
        description = {"Asks the server which files in this version it cannot serve the content for, which is how a half published or damaged version is found. Anything missing that this checkout has is uploaded, and what is still missing after that is listed.",
            "",
            "Answers in the exit code as well as the listing, so a script can act on it without reading the output."},
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {"0:every file has its content", "1:something is still missing, and is listed",
            "2:the command could not be run as given"})
public class VerifyCommand extends ConnectedCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.verify(this);
        return 0;
    }
}
