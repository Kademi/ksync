package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Its own command rather than a flag on login, because it is often wanted when logging in is not. */
@Command(name = "logout",
        header = "Discard the stored credentials for a site.",
        description = {"Removes both ways of being signed in, the OAuth tokens and the cookie login, because leaving one",
            "behind is not a logout. The client registration for this machine is kept, so the next login does",
            "not have to register again.",
            "",
            "Run inside a checkout to log out of its site, or name any site with --url."})
public class LogoutCommand extends BaseCommand {

    /** Filled from the checkout when there is one, so inside a project this is just "ksync3 logout". */
    @Option(names = {"--url"}, required = true, paramLabel = "<url|domain>",
            description = "The site to log out of, as a url or a bare domain. Taken from the checkout when run inside one")
    public String url;

    @Override
    protected Integer run() throws Exception {
        KSync3.logout(this);
        return 0;
    }
}
