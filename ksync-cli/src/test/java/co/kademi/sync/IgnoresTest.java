package co.kademi.sync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * The cases are ksync-go's, deliberately: the two clients read the same file and a checkout must
 * behave the same whichever one touched it last. A path written with a trailing slash is a
 * directory.
 */
public class IgnoresTest {

    private void check(List<String> patterns, String[] ignored, String[] kept) {
        List<String> all = new ArrayList<>(Ignores.BUILT_IN);
        all.addAll(patterns);
        Ignores ignores = Ignores.of(all);
        for (String p : ignored) {
            boolean isDir = p.endsWith("/");
            String path = isDir ? p.substring(0, p.length() - 1) : p;
            assertTrue(path + " (dir=" + isDir + ") should be ignored, patterns=" + patterns,
                    ignores.ignored(path, isDir));
        }
        for (String p : kept) {
            boolean isDir = p.endsWith("/");
            String path = isDir ? p.substring(0, p.length() - 1) : p;
            assertFalse(path + " (dir=" + isDir + ") should be kept, patterns=" + patterns,
                    ignores.ignored(path, isDir));
        }
    }

    private static List<String> patterns(String... p) {
        return Arrays.asList(p);
    }

    @Test
    public void aBareName_matchesAtAnyDepth() {
        check(patterns("node_modules"),
                new String[]{"node_modules", "a/node_modules", "a/b/node_modules/c/d.js"},
                new String[]{"node_modules.txt", "a/my_node_modules"});
    }

    @Test
    public void aLeadingSlash_anchorsToTheRoot() {
        check(patterns("/build"),
                new String[]{"build", "build/out.bin"},
                new String[]{"src/build", "src/build/out.bin"});
    }

    @Test
    public void aSlashInTheMiddle_anchorsToo() {
        check(patterns("src/generated"),
                new String[]{"src/generated", "src/generated/x.js"},
                new String[]{"lib/src/generated"});
    }

    @Test
    public void aTrailingSlash_meansDirectoriesOnly() {
        // a file called dist is content, a directory called dist is build output
        check(patterns("dist/"), new String[]{"dist/", "dist/app.js"}, new String[]{"dist"});
    }

    @Test
    public void aSingleStar_stopsAtASeparator() {
        check(patterns("*.log"),
                new String[]{"a.log", "deep/inside/b.log"},
                new String[]{"a.log.keep", "log"});
        check(patterns("logs/*.log"), new String[]{"logs/a.log"}, new String[]{"logs/nested/a.log"});
    }

    @Test
    public void doubleStar_crossesDirectories() {
        check(patterns("logs/**/*.log"),
                new String[]{"logs/a.log", "logs/nested/deep/a.log"},
                new String[]{"a.log"});
    }

    @Test
    public void aLaterRule_reIncludes() {
        check(patterns("*.log", "!keep.log"),
                new String[]{"a.log", "deep/a.log"},
                new String[]{"keep.log", "deep/keep.log"});
    }

    @Test
    public void orderMatters_lastMatchWins() {
        check(patterns("!keep.log", "*.log"), new String[]{"keep.log"}, new String[]{});
    }

    @Test
    public void questionMark_isOneCharacterAndNotASeparator() {
        check(patterns("file?.txt"),
                new String[]{"file1.txt", "a/fileX.txt"},
                new String[]{"file.txt", "file12.txt", "file/.txt"});
    }

    @Test
    public void characterClasses_includingNegation() {
        check(patterns("log[0-9].txt", "img[!a].png"),
                new String[]{"log7.txt", "imgb.png"},
                new String[]{"logx.txt", "imga.png"});
    }

    @Test
    public void anEscapedBang_isALiteralName() {
        check(patterns("\\!important"), new String[]{"!important"}, new String[]{"important"});
    }

    /**
     * The behaviour this whole change exists for. Every dot name used to be skipped locally and
     * allowed through from the server, so a dotfile on the branch could never scan back to the
     * hash it came from.
     */
    @Test
    public void dotfiles_areOrdinaryContent() {
        check(patterns(), new String[]{},
                new String[]{".htaccess", ".env.example", ".eslintrc", ".config/settings.json"});
    }

    @Test
    public void theStateDirectory_isNotNegotiable() {
        check(patterns("!" + Ignores.STATE_DIR, "!" + Ignores.STATE_DIR + "/**"),
                new String[]{".ksync", ".ksync/ksync.properties", ".ksync/blobs/ab/cd"},
                new String[]{"sub/.ksync"});
    }

    @Test
    public void theBuiltIns_applyWithNoIgnoreFile() {
        check(patterns(),
                new String[]{".git/", ".git/config", ".svn/", ".DS_Store", "a/b/.DS_Store",
                    "node_modules", "a/node_modules/x.js", "bower_components", "Thumbs.db"},
                new String[]{".gitignore", ".gitlab-ci.yml", ".gitattributes"});
    }

    /** What the user asked for: the hard coded ones hold unless the file says otherwise. */
    @Test
    public void theBuiltIns_canBeReIncluded() {
        check(patterns("!node_modules"),
                new String[]{".git/", "Thumbs.db"},
                new String[]{"node_modules", "a/node_modules/lib.js"});
        check(patterns("!.git/"), new String[]{"node_modules"}, new String[]{".git/", ".git/config"});
    }

    @Test
    public void blankLinesAndComments_selectNothing() {
        Ignores ignores = Ignores.of("", "   ", "#a comment", "!", "/", "\t");
        assertFalse(ignores.ignored("anything.txt", false));
        assertFalse(ignores.ignored("a/b/c", true));
        // the state dir still is not syncable, whatever the file does or does not say
        assertTrue(ignores.ignored(".ksync", true));
    }

    @Test
    public void aWindowsSeparator_isTreatedAsAPathSeparator() {
        Ignores ignores = Ignores.of("logs/*.log");
        assertTrue(ignores.ignored("logs\\a.log", false));
    }
}
