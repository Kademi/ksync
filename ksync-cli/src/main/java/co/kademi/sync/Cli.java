package co.kademi.sync;

import co.kademi.sync.commands.BaseCommand;
import co.kademi.sync.commands.CheckIgnoreCommand;
import co.kademi.sync.commands.CheckoutCommand;
import co.kademi.sync.commands.IgnoreCommand;
import co.kademi.sync.commands.LoginCommand;
import co.kademi.sync.commands.LogoutCommand;
import co.kademi.sync.commands.PublishCommand;
import co.kademi.sync.commands.PullCommand;
import co.kademi.sync.commands.PushCommand;
import co.kademi.sync.commands.SyncCommand;
import co.kademi.sync.commands.VerifyCommand;
import co.kademi.sync.oauth.NotLoggedInException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;

/**
 * The command line: {@code ksync3 <command> [options]}.
 *
 * Each command is a class in {@link co.kademi.sync.commands} and takes the
 * options it mixes in, so a flag meant for publish is an error on a pull rather
 * than silently accepted, and help for a command lists only that command's
 * options.
 *
 * Every option is spelled both ways, -url and --url, because the single dash
 * form is what the documentation, the existing scripts and the ksync:// links
 * all use.
 */
public class Cli {

    private static final Logger log = LoggerFactory.getLogger(Cli.class);

    @Command(name = "ksync3",
            header = "Syncs a local directory with a Kademi repository branch.",
            description = {
                "Keeps a folder on this machine and a branch of a Kademi repository in step, in both",
                "directions. A checkout records the branch it belongs to in a .ksync folder, so after",
                "the first run every command works out for itself where to talk to and who as.",
                "",
                "What a sync leaves out is decided by gitignore rules, in four layers, each able to",
                "undo the one before it: the built in defaults, then ~/.config/ksync/ignore for this",
                "machine, then a .ksyncignore in the checkout for the whole team, then --ignore for one",
                "run. Ask check-ignore which of them applied to a file."},
            footerHeading = "%nExamples:%n",
            footer = {
                "  ksync3 checkout --url https://site.kademi.co/repositories/mysite/version1",
                "  ksync3 sync",
                "  ksync3 check-ignore theme/dist/bundle.js"},
            subcommands = {
                CheckoutCommand.class, PushCommand.class, PullCommand.class,
                SyncCommand.class, VerifyCommand.class, LoginCommand.class, LogoutCommand.class,
                IgnoreCommand.class, CheckIgnoreCommand.class,
                PublishCommand.class
            },
            usageHelpAutoWidth = true,
            mixinStandardHelpOptions = true,
            versionProvider = VersionProvider.class,
            synopsisSubcommandLabel = "<command>")
    public static class Root {
    }

    /**
     * Values a command was not given come from the checkout's properties file,
     * which is what lets url be required without every run repeating it.
     * Picocli consults this again after parsing, so -appdir is known by the
     * time the directory has to be resolved.
     */
    static class CheckoutDefaults implements IDefaultValueProvider {

        @Override
        public String defaultValue(ArgSpec arg) {
            if (!(arg instanceof OptionSpec) || !(arg.command().userObject() instanceof BaseCommand)) {
                return null;
            }
            String name = StringUtils.stripStart(((OptionSpec) arg).longestName(), "-");
            // Only the keys a checkout actually records. The file is checkout metadata, not a
            // config file for every flag, and a dynamic default is a trap for a negatable option:
            // picocli toggles those against their default, so supplying true here would make both
            // --localwins and --no-localwins mean the opposite of what they say.
            if (!"url".equals(name) && !"user".equals(name) && !"ignore".equals(name)) {
                return null;
            }
            Properties props = ((BaseCommand) arg.command().userObject()).props();
            if ("url".equals(name)) {
                // A checkout that follows a repository records both, and the repository is the one
                // to start from, or the version it settled on last time would pin it there forever.
                String repoUrl = props.getProperty("repoUrl");
                return StringUtils.isNotBlank(repoUrl) ? repoUrl : props.getProperty("url");
            }
            return props.getProperty(name);
        }
    }

    /**
     * Moves a {@code -command x} pair to the front as a bare command, because
     * ksync:// links and older scripts are out in the world where they cannot
     * be edited.
     */
    static String[] commandFirst(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("-command".equals(args[i]) || "--command".equals(args[i])) {
                List<String> out = new ArrayList<>();
                out.add(args[i + 1]);
                for (int j = 0; j < args.length; j++) {
                    if (j != i && j != i + 1) {
                        out.add(args[j]);
                    }
                }
                return out.toArray(String[]::new);
            }
        }
        return args;
    }

    /**
     * A missing url usually means the command was run outside a checkout, which
     * picocli's own wording does not say.
     */
    private static CommandLine.IParameterExceptionHandler urlAwareErrors(CommandLine.IParameterExceptionHandler wrapped) {
        return (ex, args) -> {
            CommandLine cl = ex.getCommandLine();
            if (ex instanceof CommandLine.MissingParameterException
                    && ex.getMessage() != null && ex.getMessage().contains("--url")) {
                if ("logout".equals(cl.getCommandName())) {
                    cl.getErr().println("Not in a ksync checkout, so there is no site to log out of.");
                    cl.getErr().println("Name one with --url acme.kademi.co, or run this from a checkout.");
                } else {
                    cl.getErr().println(System.getProperty("user.dir")
                            + " is not a ksync checkout, so there is no url to sync with.");
                    cl.getErr().println("Give one with --url https://your-site/repositories/myrepo/version1,"
                            + " or check the branch out here first.");
                }
                return cl.getCommandSpec().exitCodeOnInvalidInput();
            }
            return wrapped.handleParseException(ex, args);
        };
    }

    /**
     * A dead login and a setup mistake both say what to do about them, and a
     * stack trace only buries it. Anything else is a fault worth the trace.
     */
    private static CommandLine.IExecutionExceptionHandler runtimeErrors() {
        return (ex, cl, parseResult) -> {
            NotLoggedInException notLoggedIn = NotLoggedInException.find(ex);
            if (notLoggedIn != null) {
                log.error(notLoggedIn.getMessage());
                return 1;
            }
            SetupException setup = SetupException.find(ex);
            if (setup != null) {
                log.error(setup.getMessage());
                return 1;
            }
            log.error("Exception running command {} - {}", cl.getCommandName(), ex.getMessage(), ex);
            return 1;
        };
    }

    /**
     * Rewrites {@code -url} to {@code --url}.
     *
     * The single dash long form is what the documentation, the older scripts
     * and the ksync:// links all use, and those are out in the world where they
     * cannot be edited. Translating here rather than declaring both spellings
     * on every option keeps help readable: picocli renders every name an option
     * has, and cannot hide one of them.
     *
     * Only names the parser actually knows are rewritten, so a cluster of short
     * options like -dl is left alone.
     */
    static String[] longFormAliases(String[] args, Set<String> known) {
        String[] out = args.clone();
        for (int i = 0; i < out.length; i++) {
            String arg = out[i];
            if (!arg.startsWith("-") || arg.startsWith("--")) {
                continue;
            }
            int eq = arg.indexOf('=');
            String name = eq < 0 ? arg : arg.substring(0, eq);
            if (name.length() > 2 && known.contains("-" + name)) {
                out[i] = "-" + arg;
            }
        }
        return out;
    }

    /**
     * Every long option name the parser knows, across the root and its
     * subcommands.
     */
    private static Set<String> longOptionNames(CommandLine cl) {
        Set<String> names = new HashSet<>();
        for (OptionSpec option : cl.getCommandSpec().options()) {
            names.addAll(Arrays.asList(option.names()));
        }
        for (CommandLine sub : cl.getSubcommands().values()) {
            names.addAll(longOptionNames(sub));
        }
        names.removeIf(n -> !n.startsWith("--"));
        return names;
    }

    public static int run(String[] args) {
        CommandLine cl = new CommandLine(new Root())
                .setDefaultValueProvider(new CheckoutDefaults())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setExecutionExceptionHandler(runtimeErrors());
        cl.setParameterExceptionHandler(urlAwareErrors(cl.getParameterExceptionHandler()));
        return cl.execute(longFormAliases(commandFirst(args), longOptionNames(cl)));
    }
}
