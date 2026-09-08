package co.kademi.sync;

import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * The JSON here is the shape VfsMissingData serialises to on the server, so a change to that class
 * which this cannot read shows up as a failure here rather than as an empty report in the field.
 */
public class MissingObjectsTest {

    private static final String CLEAN
            = "{\"status\":true,\"messages\":[\"No missing objects\"],"
            + "\"data\":{\"missingBlobs\":[],\"missingChunkFanouts\":[],\"missingFileFanouts\":[],"
            + "\"missingDirectories\":[],\"objects\":[]}}";

    private static final String BROKEN
            = "{\"status\":true,\"messages\":[\"Found 3 missing objects\"],"
            + "\"data\":{\"missingBlobs\":[\"abc0102b\"],\"missingChunkFanouts\":[],"
            + "\"missingFileFanouts\":[\"def0102b\"],\"missingDirectories\":[\"999aaa\"],"
            + "\"objects\":["
            + "{\"type\":\"directory\",\"path\":\"/theme/parts\",\"hash\":\"999aaa\"},"
            + "{\"type\":\"fileFanout\",\"path\":\"/theme/site.css\",\"hash\":\"def0102b\"},"
            + "{\"type\":\"blob\",\"path\":\"/images/logo.png\",\"hash\":\"abc0102b\"}]}}";

    private String report(String json) {
        return String.join("\n", MissingObjects.parse(json).report());
    }

    @Test
    public void nothingMissing_readsAsEmpty() {
        MissingObjects missing = MissingObjects.parse(CLEAN);
        assertTrue(missing.isEmpty());
        assertEquals(0, missing.getObjects().size());
        assertEquals("Nothing missing. Every object this version needs is on the server", report(CLEAN));
    }

    @Test
    public void missingObjects_areAttributedToTheirFiles() {
        List<MissingObjects.MissingObject> objects = MissingObjects.parse(BROKEN).getObjects();
        assertEquals(3, objects.size());
        assertEquals("/theme/site.css", objects.get(1).getPath());
        assertEquals(MissingObjects.TYPE_FILE_FANOUT, objects.get(1).getType());
        assertEquals("def0102b", objects.get(1).getHash());
    }

    @Test
    public void report_groupsByFile_andCountsByType() {
        String report = report(BROKEN);
        assertTrue(report, report.contains("2 files are missing objects:"));
        assertTrue(report, report.contains("  /theme/site.css\n      fileFanout   def0102b"));
        assertTrue(report, report.contains("Missing: 1 blob, 0 chunk fanouts, 1 file fanout, 1 directory"));
    }

    @Test
    public void aMissingDirectory_isReportedApartFromTheFiles() {
        // the walk cannot see past it, so its subtree is unchecked rather than known to be fine
        String report = report(BROKEN);
        assertTrue(report, report.startsWith("1 directory listing is missing, so nothing below could be checked:"));
        assertTrue(report, report.contains("  /theme/parts\n      directory    999aaa"));
        // and it is not counted among the files
        assertFalse(report, report.contains("3 files are missing objects"));
        assertFalse(report(clean(1)), report(clean(1)).contains("directory listing is missing"));
    }

    @Test
    public void oneOfEach_readsAsSingular() {
        String report = report(clean(1));
        assertTrue(report, report.startsWith("1 file is missing objects:"));
        assertTrue(report, report.contains("Missing: 1 blob, 0 chunk fanouts, 0 file fanouts, 0 directories"));
    }

    @Test
    public void manyChunksOfOneFile_areCollapsed() {
        // a large file is split into hundreds of chunks; listing them all would bury every other file
        String report = report(clean(9));
        assertTrue(report, report.contains("(and 4 more blobs)"));
        assertEquals(5, report.split("\n      blob").length - 1);
    }

    @Test
    public void objectsOfMixedTypes_collapseWithoutNamingOne() {
        StringBuilder objects = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            String type = i % 2 == 0 ? "blob" : "chunkFanout";
            objects.append(i > 0 ? "," : "").append("{\"type\":\"").append(type)
                    .append("\",\"path\":\"/a.js\",\"hash\":\"h").append(i).append("\"}");
        }
        // the three past the cap are not all of one kind, so the collapsed line cannot name one
        assertTrue(report("{\"status\":true,\"data\":{\"objects\":[" + objects + "]}}").contains("(and 3 more objects)"));
    }

    @Test
    public void flatListsWithoutAttribution_areStillReported() {
        // better than reporting a broken version as clean, if the server ever answers this way
        String report = report("{\"status\":true,\"data\":{\"missingBlobs\":[\"aaa\"],\"objects\":[]}}");
        assertTrue(report, report.contains("(the server did not say which file)"));
        assertTrue(report, report.contains("Missing: 1 blob"));
    }

    @Test
    public void anHtmlResponse_saysTheServerIsTooOld() {
        // an older server does not know the parameter and renders the branch page instead
        String msg = parseFails("<!DOCTYPE html>\n<html><body>Version 1.4.6</body></html>");
        assertTrue(msg, msg.contains("older server"));
        assertTrue(msg, msg.contains("24539"));
    }

    @Test
    public void aRefusal_reportsWhatTheServerSaid() {
        String msg = parseFails("{\"status\":false,\"messages\":[\"This version has no content to check\"]}");
        assertTrue(msg, msg.contains("This version has no content to check"));
    }

    @Test
    public void anEmptyResponse_saysSo() {
        assertTrue(parseFails("").contains("empty response"));
    }

    private String parseFails(String response) {
        try {
            MissingObjects.parse(response);
            fail("Expected a SetupException");
            return null;
        } catch (SetupException ex) {
            return ex.getMessage();
        }
    }

    /** A response with n missing blobs, all belonging to one file. */
    private String clean(int n) {
        StringBuilder objects = new StringBuilder();
        for (int i = 0; i < n; i++) {
            objects.append(i > 0 ? "," : "").append("{\"type\":\"blob\",\"path\":\"/js/app.js\",\"hash\":\"h").append(i).append("\"}");
        }
        return "{\"status\":true,\"data\":{\"objects\":[" + objects + "]}}";
    }
}
