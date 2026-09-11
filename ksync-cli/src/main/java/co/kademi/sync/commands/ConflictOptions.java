package co.kademi.sync.commands;

import io.milton.sync.ConflictResolvers;
import picocli.CommandLine.Option;

/**
 * What the commands that move files and resolve conflicts take.
 */
public class ConflictOptions {

    @Option(names = {"-l", "--localwins"}, negatable = true,
            description = "Treat local as authoritative: overwrite the remote even when it has changed, and keep the local file on a conflict, without prompting. For a checkout that is version managed. Makes --conflictmode irrelevant, because nothing is asked")
    public boolean localwins;

    @Option(names = {"-c", "--conflictmode"}, defaultValue = "AUTO",
            description = "How to ask about file conflicts: gui (a dialog, the default), console (a terminal prompt, for CI or an agent), or auto")
    public ConflictResolvers.Mode conflictmode;

    @Option(names = {"-s", "--statusfile"}, paramLabel = "<file>",
            description = "Where to write the JSON status file that a status bar, editor or script can read. Defaults to .ksync/status.json inside the checkout")
    public String statusfile;
}
