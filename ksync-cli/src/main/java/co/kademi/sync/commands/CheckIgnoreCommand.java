package co.kademi.sync.commands;

import co.kademi.sync.Ignores;
import java.io.File;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Why a path is or is not synced, the counterpart to git check-ignore.
 *
 * With four layers of ignore file and rules that can undo each other, "which rule won, and which
 * file did it come from" stops being answerable by reading them.
 *
 * Unlike git this reports the rule by default, because the question being asked is always why.
 * Exit codes match git: 0 if any path given is ignored, 1 if none is.
 */
@Command(name = "check-ignore",
        header = "Show which ignore rule excludes a path.",
        description = {"Answers \"why is this file not syncing\" by naming the rule that decided it, the file that",
            "rule came from, and the line number.",
            "",
            "A path that a rule put back is reported too and marked as re-included, because that is a",
            "different answer from no rule having matched at all."},
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {"0:at least one path given is ignored", "1:none of them is",
            "2:a path was outside the checkout, or an option was wrong"})
public class CheckIgnoreCommand extends BaseCommand {

    @Parameters(arity = "1..", paramLabel = "<path>",
            description = "Paths to check, relative to the checkout or absolute")
    public List<File> paths;

    @Option(names = {"-i", "--ignore"}, paramLabel = "<patterns>",
            description = "Extra patterns to apply, as a sync would take them")
    public String ignore;

    @Option(names = {"-n", "--non-matching"},
            description = "Also report paths that no rule matched")
    public boolean nonMatching;

    @Option(names = {"-q", "--quiet"},
            description = "Print nothing, and say the answer in the exit code")
    public boolean quiet;

    @Override
    protected Integer run() throws Exception {
        File root = dir();
        Ignores ignores = Ignores.load(root, co.kademi.sync.KSync3Utils.split(ignore));
        boolean anyIgnored = false;

        for (File given : paths) {
            String rel = relativize(root, given.getPath());
            if (rel == null) {
                throw fail(given.getPath() + " is outside the checkout at " + root.getAbsolutePath());
            }
            boolean isDir = new File(root, rel).isDirectory();
            Ignores.Match match = ignores.explain(rel, isDir);
            boolean ignored = match != null && match.ignored;
            anyIgnored |= ignored;

            if (quiet) {
                continue;
            }
            if (ignored) {
                System.out.println(match + "\t" + rel);
            } else if (match != null) {
                // a "!" rule won, which is worth showing: it is why an exclusion did not apply
                System.out.println(match + "\t" + rel + "\t(re-included)");
            } else if (nonMatching) {
                System.out.println("::\t" + rel);
            }
        }
        return anyIgnored ? 0 : 1;
    }

    /** @return the path relative to the checkout root, or null if it is outside */
    public static String relativize(File root, String given) {
        File file = new File(given);
        if (!file.isAbsolute()) {
            return given.replace(File.separatorChar, '/');
        }
        String rootPath = root.getAbsoluteFile().toPath().normalize().toString();
        String filePath = file.toPath().normalize().toString();
        if (filePath.equals(rootPath)) {
            return "";
        }
        if (!filePath.startsWith(rootPath + File.separator)) {
            return null;
        }
        return filePath.substring(rootPath.length() + 1).replace(File.separatorChar, '/');
    }
}
