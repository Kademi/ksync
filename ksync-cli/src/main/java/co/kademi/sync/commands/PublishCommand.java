package co.kademi.sync.commands;

import co.kademi.deploy.AppDeployer;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "publish", header = "Publish apps, libs or themes to the marketplace.",
        description = {"Run from the folder that holds the apps, libs and themes directories, and name what to publish",
            "with --appids. Each asset must have exactly one version folder inside it.",
            "",
            "Not needed to use an app or lib within your own account, only to list it on the marketplace."})
public class PublishCommand extends ConnectedCommand {

    /** Nowhere to remember this: which apps to publish is decided per run. */
    @Option(names = {"-a", "--appids"}, required = true, paramLabel = "<ids>", split = ",",
            description = "Which apps to publish. Asterisk for all; or a comma separated list of ids; or absolute paths, eg * ; or /libs; or leadman-lib, payment-lib")
    public List<String> appIds;

    @Option(names = {"-f", "--force"}, description = "Update already published apps")
    public boolean force;

    @Option(names = {"-r", "--report"}, description = "Display report only, do not make changes")
    public boolean report;

    @Override
    protected Integer run() throws Exception {
        AppDeployer.publish(this);
        return 0;
    }
}
