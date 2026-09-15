package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sync", header = "Watch for local changes and push each one as it happens.",
        description = {"Runs until stopped, pushing each change as it is saved. This is the command to leave running while you work.",
            "",
            "If it cannot get going - the server is unreachable, the url is not a branch, or the first push fails - it says why and exits, rather than sitting there watching a checkout it cannot push.",
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
        // the watcher runs on its own threads, so hold this one until it is interrupted
        try {
            while (true) {
                Thread.sleep(200);
            }
        } catch (InterruptedException ex) {
            return 0;
        }
    }
}
