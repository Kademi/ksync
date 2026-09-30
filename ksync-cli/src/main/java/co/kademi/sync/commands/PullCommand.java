package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "pull", header = "Download remote changes into the local working copy.",
        description = {"Applies everything that changed on the branch since the last sync. A file changed on both sides is a conflict, and you are asked about it unless --localwins says to keep yours.",
            "",
            "A conflict left unanswered, with n or because there is no console, keeps your file and leaves the pull unrecorded, so push stays blocked until a later pull settles it.",
            "",
            "Exits 1 when the pull did not complete: the server could not be reached, some files could not be brought down, or a conflict was left unanswered."})
public class PullCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.pull(this);
        return 0;
    }
}
