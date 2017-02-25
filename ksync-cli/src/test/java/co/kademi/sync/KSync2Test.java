package co.kademi.sync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class KSync2Test extends TestCase {

    public KSync2Test(String testName) {
        super(testName);
    }

    @Test
    public void test() throws IOException {
        List<String> missingBlobs = new ArrayList<>();
        List<String> missingFileFanouts = new ArrayList<>();
        List<String> missingChunkFanouts = new ArrayList<>();

        missingBlobs.add("blob1");
        missingBlobs.add("blob2");

        missingFileFanouts.add("file1");

        Map<String, List> errors = new HashMap<>();
        errors.put("missingBlobs", missingBlobs);
        errors.put("missingChunkFanouts", missingChunkFanouts);
        errors.put("missingFileFanouts", missingFileFanouts);
        JsonResult jsonResult = new JsonResult(false, "Missing VFS data");
        jsonResult.setData(errors);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        jsonResult.write(bout);

        String json = bout.toString();

        JSONObject jsonRes = JSONObject.fromObject(json);

        Object dataOb = jsonRes.get("data");
        JSONObject data = (JSONObject) dataOb;
        JSONArray missingChunksArr = (JSONArray) data.get("missingChunkFanouts");
        assertEquals(0, missingChunksArr.size());
        JSONArray missingBlobsArr = (JSONArray) data.get("missingBlobs");
        assertEquals(2, missingBlobsArr.size());
        JSONArray missingFileFanoutsArr = (JSONArray) data.get("missingFileFanouts");
        assertEquals(1, missingFileFanoutsArr.size());
        System.out.println("data: " + dataOb);

    }

}
