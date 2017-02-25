package co.kademi.sync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Map;
import net.sf.json.JSON;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;
import net.sf.json.util.CycleDetectionStrategy;
import org.slf4j.LoggerFactory;

/**
 * Just writes an object to the outputstream in JSON notation.
 *
 * Normally used with JsonResult
 *
 * @author brad
 */
public class JsonWriter {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(JsonWriter.class);

    public static String toJsonText(Object o) {
        JsonWriter w = new JsonWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            w.writeWithNiceDates(o, out);
            return out.toString();
        } catch (IOException ex) {
            log.warn("Couldnt parse json data to text", ex);
            return null;
        }

    }

    public static Object parse(String s) {
        JsonConfig cfg = new JsonConfig();
        cfg.setIgnoreTransientFields(true);
        cfg.setCycleDetectionStrategy(CycleDetectionStrategy.LENIENT);

        JSON json = JSONSerializer.toJSON(s);
        return JSONSerializer.toJava(json);
    }

    public void write(Object object, OutputStream out) throws IOException {
        JsonConfig cfg = new JsonConfig();
        cfg.setAllowNonStringKeys(true);
        cfg.setArrayMode(JsonConfig.MODE_OBJECT_ARRAY);
        cfg.setIgnoreTransientFields(true);
        cfg.setCycleDetectionStrategy(CycleDetectionStrategy.LENIENT);

        JSON json = JSONSerializer.toJSON(object, cfg);
        Writer writer = new PrintWriter(out);
        json.write(writer);
        writer.flush();
    }

    public void write(Object object, OutputStream out, JsonConfig cfg) throws IOException {
        JSON json = JSONSerializer.toJSON(object, cfg);
        Writer writer = new PrintWriter(out);
        json.write(writer);
        writer.flush();
    }

    public void writeWithNiceDates(Object object, OutputStream out) throws IOException {
        JsonConfig cfg = new JsonConfig();
        try {
            cfg.registerJsonValueProcessor(
                    Class.forName("java.util.Date"),
                    new NiceDateJsonBeanProcessor());
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        cfg.setIgnoreTransientFields(true);
        cfg.setCycleDetectionStrategy(CycleDetectionStrategy.LENIENT);

        JSON json = JSONSerializer.toJSON(object, cfg);
        Writer writer = new PrintWriter(out);
        json.write(writer);
        writer.flush();
    }

    public String toString(Object object) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            write(object, bout);
            return bout.toString("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class NiceDateJsonBeanProcessor implements JsonValueProcessor {

        @Override
        public Object processArrayValue(Object value, JsonConfig jsonConfig) {
            return process(value, jsonConfig);
        }

        @Override
        public Object processObjectValue(String key, Object value, JsonConfig jsonConfig) {
            return process(value, jsonConfig);
        }

        private Object process(Object value, JsonConfig jsonConfig) {
            if (value instanceof java.util.Date) {
                java.util.Date d = (java.util.Date) value;
                return d.getTime();
            }
            return null;
        }
    }
}
