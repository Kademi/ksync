package co.kademi.sync.commands;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/** What every command that talks to a server takes. */
public class ConnectionOptions {

    @Option(names = {"--url"}, required = true, paramLabel = "<url>",
            description = "The branch to sync with, eg https://site.kademi.co/repositories/mysite/version1. Remembered in the checkout, so only a first run needs it")
    public String url;

    @ArgGroup(exclusive = true, heading = "%nAuthentication, when no login is stored for the site yet:%n")
    public Credentials credentials;

    /**
     * Outside the exclusive group on purpose: an option inside one loses its default when the
     * group goes unmentioned, which would break the environment fallback.
     */
    @Option(names = {"-t", "--token"}, paramLabel = "<apikey>", defaultValue = "${env:KSYNC_TOKEN}",
            description = "A KOAuth2 api key (ko2_ak_...) to authenticate with, used as given and never stored. Defaults to the KSYNC_TOKEN environment variable")
    public String apiKey;

    @Option(names = {"-i", "--ignore"}, paramLabel = "<patterns>",
            description = "Comma separated file/folder name patterns to ignore for this run, on top of the built in ones and your ignore file. See the ignore command")
    public String ignore;

    /** Null unless a username was given: picocli leaves an unmatched group unset. */
    public String user() {
        return credentials == null || credentials.userPassword == null ? null : credentials.userPassword.user;
    }

    public String password() {
        return credentials == null || credentials.userPassword == null ? null : credentials.userPassword.password;
    }

    public String authToken() {
        return credentials == null ? null : credentials.authToken;
    }
}
