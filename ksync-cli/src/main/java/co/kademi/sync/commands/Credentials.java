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
            description = "An encrypted token from the server which provides authentication, or a KOAuth2 api key (ko2_ak_...) the same as --token")
    public String authToken;

    /** No ${env:KSYNC_TOKEN} default: picocli skips an unmatched group's defaults, and OAuth2Client reads it anyway. */
    @Option(names = {"-t", "--token"}, paramLabel = "<apikey>",
            description = "A KOAuth2 api key (ko2_ak_...) to authenticate with, used as given and never stored. Defaults to the KSYNC_TOKEN environment variable")
    public String apiKey;
}
