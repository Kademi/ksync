package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Not about any one checkout, so it needs no url and no login. */
@Command(name = "ignore", header = "Add patterns to your ignore file, or list what is in it.",
        description = {"Edits the ignore file that applies to every checkout on this machine. Rules the whole team",
            "should share belong in a .ksyncignore inside the checkout instead, which is synced like any",
            "other file.",
            "",
            "The syntax is gitignore's. Run with no pattern to list what is in force."})
public class IgnoreCommand extends BaseCommand {

    @Option(names = {"--pattern"}, paramLabel = "<glob>",
            description = "File/folder name or glob to add, eg *.log or node_modules. Comma separated for several. Lists the file when omitted")
    public String pattern;

    @Override
    protected Integer run() throws Exception {
        KSync3.ignore(this);
        return 0;
    }
}
