package co.kademi.sync.commands;

import co.kademi.deploy.AppDeployer;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "publish", header = "Publish apps, libs, themes or recipes to the marketplace.",
        description = {"Run from the folder that holds the apps, libs, themes and recipes directories, and name what to publish with --appids. Each asset must have exactly one version folder inside it.",
            "",
            "Not needed to use an app or lib within your own account, only to list it on the marketplace.",
            "",
            "A ksync.toml in the folder sets the order: its tiers are published top to bottom, and each tier's first entries before the rest of it. Without one, the order is themes, apps, libs, recipes.",
            "",
            "Exits 1 when anything still failed after its retries."})
public class PublishCommand extends ConnectedCommand {

    /** Nowhere to remember this: which apps to publish is decided per run. */
    // Split on the surrounding space too: the ids are compared with equals, and the help itself
    // shows the list written with spaces after the commas.
    @Option(names = {"-a", "--appids"}, required = true, paramLabel = "<ids>", split = "\\s*,\\s*",
            description = "Which apps to publish. Asterisk for all; or a comma separated list of ids; or absolute paths, eg * ; or /libs; or leadman-lib, payment-lib")
    public List<String> appIds;

    @Option(names = {"-f", "--force"}, description = "Update already published apps")
    public boolean force;

    @Option(names = {"-r", "--report"}, description = "Display report only, do not make changes")
    public boolean report;

    @Option(names = {"--retries"}, defaultValue = "3", paramLabel = "<n>",
            description = "How many more times to try an app, lib or theme that fails, with --force from the second try. Default ${DEFAULT-VALUE}")
    public int retries;

    @Override
    protected Integer run() throws Exception {
        AppDeployer.publish(this);
        return 0;
    }
}
