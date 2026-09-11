package co.kademi.sync.commands;

import co.kademi.sync.KSync3;
import co.kademi.sync.KSync3Utils;
import co.kademi.sync.KSyncUtils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * The options every command shares, and the one place the shared setup runs.
 */
public abstract class BaseCommand implements Callable<Integer> {

    @Spec
    public CommandSpec spec;

    @Mixin
    public GlobalOptions global = new GlobalOptions();

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean helpRequested;

    private Properties cachedProps;
    private String cachedFor;

    /**
     * The directory this command works on. Read only: nothing here creates it.
     */
    public File dir() {
        return KSync3Utils.checkoutDir(global.appdir, global.appname);
    }

    /**
     * The checkout's saved properties, empty when this is not a checkout yet. Cached per
     * directory, because picocli asks for defaults before the command line is parsed and again
     * after, and only the second answer knows where -appdir pointed.
     */
    public Properties props() {
        String path = dir().getAbsolutePath();
        if (!path.equals(cachedFor)) {
            cachedProps = KSyncUtils.readProps(new File(dir(), ".ksync"));
            cachedFor = path;
        }
        return cachedProps;
    }

    /**
     * Reported like a missing required option: message, usage hint, exit 2.
     *
     * @param message
     */
    public ParameterException fail(String message) {
        return new ParameterException(spec.commandLine(), message);
    }

    /**
     * The conflict and status options, for the commands that have them.
     */
    public ConflictOptions conflict() {
        return null;
    }

    /**
     * Whether the status bar icon was turned off, which only the sync command
     * can do.
     */
    public boolean trayDisabled() {
        return false;
    }

    @Override
    public Integer call() throws Exception {
        KSync3.configure(this);
        return run();
    }

    protected abstract Integer run() throws Exception;
}
