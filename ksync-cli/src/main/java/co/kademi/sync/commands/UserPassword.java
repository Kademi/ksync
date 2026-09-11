package co.kademi.sync.commands;

import picocli.CommandLine.Option;

/** A password without a username is not a way in, so the username is required once either appears. */
public class UserPassword {

    @Option(names = {"-u", "--user"}, required = true, paramLabel = "<user>",
            description = "Username to log in with, not your email address")
    public String user;

    @Option(names = {"-p", "--password"}, paramLabel = "<password>",
            description = "Password to log in with. Prompted for when it is left out")
    public String password;
}
