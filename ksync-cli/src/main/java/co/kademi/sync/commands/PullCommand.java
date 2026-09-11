package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "pull", header = "Download remote changes into the local working copy.",
        description = {"Applies everything that changed on the branch since the last sync. A file changed on both sides",
            "is a conflict, and you are asked about it unless --localwins says to keep yours."})
public class PullCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.pull(this);
        return 0;
    }
}
