package co.kademi.deploy;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.FanoutSerializationUtils;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class AppDeployerUtils {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AppDeployerUtils.class);

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

    public static byte[] compressBulkFanouts(Set<AppDeployer.FanoutBean> toUpload) throws Exception{
        ByteArrayOutputStream dest = new ByteArrayOutputStream();
        ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));
        log.info("compressBulkFanouts: fanouts to upload={}", toUpload.size());
        for (AppDeployer.FanoutBean fanout : toUpload) {
            log.info("compressBulkFanouts: hash={}", fanout.hash);
            ZipEntry zipEntry = new ZipEntry(fanout.hash );

            out.putNextEntry(zipEntry);
            ByteArrayOutputStream fanoutOut = new ByteArrayOutputStream();
            FanoutSerializationUtils.writeFanout(fanout.blobHashes, fanout.actualContentLength, fanoutOut);
            byte[] bytes = fanoutOut.toByteArray();
            String s = new String(bytes);
            IOUtils.write(bytes, out);
        }

        out.flush();
        out.finish();

        IOUtils.closeQuietly(out);

        return dest.toByteArray();
    }

}
