package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import co.kademi.sync.StopKey;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sync", header = "Watch for local changes and push each one as it happens.",
        description = {"Runs until stopped, pushing each change as it is saved. This is the command to leave running while you work.",
            "",
            "If it cannot get going - the server is unreachable, the url is not a branch, or the first push fails - it says why and exits, rather than sitting there watching a checkout it cannot push.",
            "",
            "Stop it with q and Enter, or ctrl-c, or Quit from the status icon. The q is there because ctrl-c does not always reach the program on Windows.",
            "",
            "Progress is written to .ksync/status.json for an editor or status bar to read, and shown in the OS status bar unless --notray."})
public class SyncCommand extends SyncingCommand {

    @Option(names = {"--notray"},
            description = "Do not show the status icon in the OS status bar. The status file is still written")
    public boolean notray;

    @Override
    public boolean trayDisabled() {
        return notray;
    }

    @Override
    protected Integer run() throws Exception {
        KSync3.sync(this);
        // the watcher runs on its own threads, so hold this one until it is told to stop
        return StopKey.waitUntilStopped();
    }
}
