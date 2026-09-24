package co.kademi.sync.commands;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/** How to sign in. Exclusive, for the same reason {@link Credentials} is. */
public class LoginMethod {

    @Option(names = {"-o", "--oauth"},
            description = "Sign in by browser only, with no fallback to a username and password")
    public boolean oauth;

    @ArgGroup(exclusive = false)
    public UserPassword userPassword;
}
