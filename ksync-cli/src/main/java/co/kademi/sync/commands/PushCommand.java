package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "push", header = "Upload local changes, if the branch has not moved on.",
        description = "Sends everything changed here since the last push. If the branch has moved on since then, the push stops and tells you to pull first, so it cannot quietly discard work someone else pushed. --localwins pushes anyway and overwrites the remote with what is here.")
public class PushCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.push(this);
        return 0;
    }
}
