package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Checkout without the download: the folder is associated with a branch and the site is signed
 * into, and nothing is written into the working directory.
 *
 * This is for the two cases a checkout is wrong for - a folder which already holds the files, and
 * one which is about to be filled from somewhere else, like a git clone - where the download is
 * either a waste or something to be undone.
 */
@Command(name = "init", header = "Set this directory up to sync with a branch, without downloading it.",
        description = {"Records the branch in .ksync and makes sure there is a login for the site, so that later commands here need no url and no password. Unlike checkout, no files are fetched: what is already in the directory is left exactly as it is.",
            "",
            "Run in a directory already set up, it reports what that is and changes nothing.",
            "",
            "Give a url with no version on the end to follow the repository, and its latest version is resolved and reported. With no url at all, you are asked for one.",
            "",
            "Signing in is by browser unless a username is given, and falls back to a username and password when the site does not offer OAuth2 or the browser login does not complete."})
public class InitCommand extends BaseCommand {

    /**
     * Not required, unlike its siblings: being run outside a checkout is the normal way to run
     * this one, and the url is asked for when it is left out.
     */
    @Option(names = {"--url"}, paramLabel = "<url>",
            description = "The branch or repository to sync with, eg https://site.kademi.co/repositories/mysite/version1. Asked for when it is left out")
    public String url;

    @ArgGroup(exclusive = true)
    public LoginMethod method;

    /** True when a browser login was asked for by name, which means no password fallback. */
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
        return KSync3.init(this);
    }
}
