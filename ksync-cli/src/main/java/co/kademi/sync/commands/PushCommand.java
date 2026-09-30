package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "push", header = "Upload local changes, if the branch has not moved on.",
        description = {"Sends everything changed here since the last push. If the branch has moved on since then, the push stops and tells you to pull first, so it cannot quietly discard work someone else pushed. --localwins pushes anyway and overwrites the remote with what is here.",
            "",
            "Once the new version is live, the server is asked whether it has everything the version needs, and anything missing is uploaded. That walks the whole version, so on a large one push can take minutes to finish; the version is already live, so stopping it then is safe.",
            "",
            "Exits 1 when the push did not go through, including when the branch had moved on."})
public class PushCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.push(this);
        return 0;
    }
}
