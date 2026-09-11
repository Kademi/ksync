package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;

@Command(name = "checkout", header = "Download a remote branch into this directory.",
        description = {"Fetches every file in the branch and writes it here, then records the branch in .ksync so that",
            "later commands in this directory need no url.",
            "",
            "Give a url with no version on the end to follow the repository, which syncs against its latest",
            "version rather than pinning to one."})
public class CheckoutCommand extends SyncingCommand {

    @Override
    protected Integer run() throws Exception {
        KSync3.checkout(this);
        return 0;
    }
}
