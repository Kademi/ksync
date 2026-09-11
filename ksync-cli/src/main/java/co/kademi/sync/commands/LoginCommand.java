package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Creates a stored login, so unlike its siblings it takes no api key and no ignore patterns. */
@Command(name = "login", header = "Sign in to a site and store the login for later runs.",
        description = {"Credentials are stored per site, so every checkout of that site is signed in at once. With",
            "--oauth a browser is opened to authorize; otherwise a username and password are used.",
            "",
            "A KSYNC_TOKEN in the environment, or --token on the command line, is used instead of anything",
            "stored here and needs no login at all."})
public class LoginCommand extends BaseCommand {

    @Option(names = {"--url"}, required = true, paramLabel = "<url>",
            description = "The site to sign in to. Remembered in the checkout, so only a first run needs it")
    public String url;

    @ArgGroup(exclusive = true)
    public LoginMethod method;

    public boolean oauth() {
        return method != null && method.oauth;
    }

    public String user() {
        return method == null || method.userPassword == null ? null : method.userPassword.user;
    }

    public String password() {
        return method == null || method.userPassword == null ? null : method.userPassword.password;
    }

    @Override
    protected Integer run() throws Exception {
        KSync3.runLogin(this);
        return 0;
    }
}
