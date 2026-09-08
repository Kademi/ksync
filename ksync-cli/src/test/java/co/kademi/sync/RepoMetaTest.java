package co.kademi.sync;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * The JSON here is what RepositoryFolder.calcRepoMeta serialises to, so a change to that shape
 * which this cannot read shows up here rather than as a checkout quietly staying on an old version.
 */
public class RepoMetaTest {

    private static final String META
            = "{\"name\":\"KProducts\","
            + "\"liveVersion\":{\"name\":\"1.2.9\",\"hash\":\"aaa111\",\"commitId\":41,\"modifiedDate\":1757200000000,"
            + "\"createdDate\":1757100000000,\"hidden\":false,\"readonly\":false},"
            + "\"latestVersion\":{\"name\":\"1.2.15\",\"hash\":\"bbb222\",\"commitId\":42,\"modifiedDate\":1757300000000,"
            + "\"createdDate\":1757200000000,\"hidden\":false,\"readonly\":true},"
            + "\"versions\":["
            + "{\"name\":\"dev\",\"hash\":\"ccc333\",\"commitId\":40,\"modifiedDate\":null,\"createdDate\":1757000000000,\"hidden\":true,\"readonly\":false},"
            + "{\"name\":\"1.2.9\",\"hash\":\"aaa111\",\"commitId\":41,\"modifiedDate\":1757200000000,\"createdDate\":1757100000000,\"hidden\":false,\"readonly\":false},"
            + "{\"name\":\"1.2.15\",\"hash\":\"bbb222\",\"commitId\":42,\"modifiedDate\":1757300000000,\"createdDate\":1757200000000,\"hidden\":false,\"readonly\":true}]}";

    @Test
    public void latestVersion_isTakenFromTheServer() {
        // 1.2.15 over 1.2.9 is the server's component-wise comparison; re-deriving it here would
        // eventually disagree with it, and disagree silently
        RepoMeta meta = RepoMeta.parse(META);
        assertEquals("KProducts", meta.getName());
        assertEquals("1.2.15", meta.getLatestVersion().getName());
        assertEquals("bbb222", meta.getLatestVersion().getHash());
        assertTrue(meta.getLatestVersion().isReadonly());
    }

    @Test
    public void liveVersion_isReadSeparatelyFromLatest() {
        assertEquals("1.2.9", RepoMeta.parse(META).getLiveVersion().getName());
    }

    @Test
    public void everyVersion_isListed_includingOnesThatCannotBeLatest() {
        // a hand named branch cannot win, but it is still somewhere a checkout could be pointed
        assertEquals(Arrays.asList("dev", "1.2.9", "1.2.15"), RepoMeta.parse(META).versionNames());
        assertTrue(RepoMeta.parse(META).getVersions().get(0).isHidden());
    }

    @Test
    public void aRepositoryWithNoVersionNumberedBranch_hasNoLatest() {
        RepoMeta meta = RepoMeta.parse("{\"name\":\"scratch\",\"liveVersion\":null,\"latestVersion\":null,"
                + "\"versions\":[{\"name\":\"dev\",\"hash\":\"ccc333\"}]}");
        assertNull(meta.getLatestVersion());
        assertNull(meta.getLiveVersion());
        assertEquals(Arrays.asList("dev"), meta.versionNames());
    }

    @Test
    public void aVersionWithNoContent_hasNoHash() {
        RepoMeta meta = RepoMeta.parse("{\"name\":\"new\",\"latestVersion\":{\"name\":\"1.0.0\",\"hash\":null},\"versions\":[]}");
        assertEquals("1.0.0", meta.getLatestVersion().getName());
        assertNull(meta.getLatestVersion().getHash());
    }

    @Test
    public void someOtherJson_isNotRepositoryMetadata() {
        // a version can hold a file of this name, so fetch treats this as "not a repository url"
        // rather than as a repository with no versions
        assertFalse(RepoMeta.parse("{\"status\":true,\"messages\":[\"hi\"]}").isRepository());
        assertTrue(RepoMeta.parse(META).isRepository());
        assertTrue(RepoMeta.parse("{\"name\":\"empty\",\"versions\":[]}").isRepository());
    }

    @Test
    public void unreadableJson_saysSo() {
        try {
            RepoMeta.parse("{not json");
            fail("Expected a SetupException");
        } catch (SetupException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Could not read the repository metadata"));
        }
    }

    @Test
    public void metaPath_isRelativeToTheHost_withOrWithoutATrailingSlash() {
        assertEquals("/repositories/KProducts/repo-meta.json", RepoMeta.metaPath("https://acme.kademi.us/repositories/KProducts/"));
        assertEquals("/repositories/KProducts/repo-meta.json", RepoMeta.metaPath("https://acme.kademi.us/repositories/KProducts"));
        assertEquals("/repo-meta.json", RepoMeta.metaPath("https://acme.kademi.us"));
    }

    @Test
    public void versionUrl_joinsWithoutDoublingTheSlash() {
        assertEquals("https://acme.kademi.us/repositories/KProducts/1.2.15/",
                RepoMeta.versionUrl("https://acme.kademi.us/repositories/KProducts/", "1.2.15"));
        assertEquals("https://acme.kademi.us/repositories/KProducts/1.2.15/",
                RepoMeta.versionUrl("https://acme.kademi.us/repositories/KProducts", "1.2.15"));
    }

    @Test
    public void versionNameOf_readsTheVersionOutOfACheckoutUrl() {
        assertEquals("1.4.6", RepoMeta.versionNameOf("https://acme.kademi.us/repositories/KProducts/1.4.6/"));
        assertEquals("1.4.6", RepoMeta.versionNameOf("https://acme.kademi.us/repositories/KProducts/1.4.6"));
    }
}
