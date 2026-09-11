package co.kademi.sync.commands;

import picocli.CommandLine.Mixin;

/**
 * A command that talks to a server, and so needs a url and a way to
 * authenticate.
 */
public abstract class ConnectedCommand extends BaseCommand {

    @Mixin
    public ConnectionOptions connection = new ConnectionOptions();
}
