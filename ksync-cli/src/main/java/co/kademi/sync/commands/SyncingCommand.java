package co.kademi.sync.commands;

import picocli.CommandLine.Mixin;

/** A command that moves files, and so can meet a conflict and has progress worth reporting. */
public abstract class SyncingCommand extends ConnectedCommand {

    @Mixin
    public ConflictOptions conflictOptions = new ConflictOptions();

    @Override
    public ConflictOptions conflict() {
        return conflictOptions;
    }
}
