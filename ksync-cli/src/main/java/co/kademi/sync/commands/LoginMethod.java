package co.kademi.sync.commands;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/** How to sign in. Exclusive, for the same reason {@link Credentials} is. */
public class LoginMethod {

    @Option(names = {"-o", "--oauth"},
            description = "Use OAuth2 instead of a username and password. Opens a browser to authorize")
    public boolean oauth;

    @ArgGroup(exclusive = false)
    public UserPassword userPassword;
}
