package co.kademi.sync.commands;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/**
 * How to authenticate when no login is stored yet. Exclusive: a username and a server token are two
 * answers to the same question, so being given both means one would be ignored silently.
 */
public class Credentials {

    @ArgGroup(exclusive = false)
    public UserPassword userPassword;

    @Option(names = {"--auth"}, paramLabel = "<user,token>",
            description = "An encrypted token from the server which provides authentication")
    public String authToken;
}
