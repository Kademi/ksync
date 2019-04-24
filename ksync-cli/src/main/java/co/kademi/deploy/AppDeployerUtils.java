package co.kademi.deploy;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.hashsplit4j.api.BlobImpl;

/**
 *
 * @author dylan
 */
public class AppDeployerUtils {

    public static byte[] compressBulkBlobs(Collection<BlobImpl> blobs) throws IOException {
        ByteArrayOutputStream dest = new ByteArrayOutputStream();
        ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

        for (BlobImpl blob : blobs) {
            ZipEntry zipEntry = new ZipEntry(blob.getHash());

            out.putNextEntry(zipEntry);
            IOUtils.write(blob.getBytes(), out);
        }

        out.flush();
        out.finish();

        IOUtils.closeQuietly(out);

        return dest.toByteArray();
    }
}
