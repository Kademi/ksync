package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "push", header = "Upload local changes, after pulling and merging.",
        description = {"Sends everything changed here since the last sync. The remote is pulled and merged first, so a",
            "push cannot quietly discard work someone else pushed. A file changed on both sides is a",
            "conflict, and you are asked about it unless --localwins says to keep yours."})
public class PushCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.push(this);
        return 0;
    }
}
