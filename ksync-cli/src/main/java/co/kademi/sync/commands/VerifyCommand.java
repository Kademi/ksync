package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "verify",
        header = "Report files whose content is missing from the server.",
        description = {"Asks the server which files in this version it cannot serve the content for, which is how a",
            "half published or damaged version is found.",
            "",
            "Answers in the exit code as well as the listing, so a script can act on it without reading the",
            "output."},
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {"0:every file has its content", "1:something is missing, and is listed",
            "2:the command could not be run as given"})
public class VerifyCommand extends ConnectedCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.verify(this);
        return 0;
    }
}
