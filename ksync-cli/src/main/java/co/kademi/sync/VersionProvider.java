package co.kademi.sync;

import java.net.JarURLConnection;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import picocli.CommandLine.IVersionProvider;

/**
 * What the version option prints, eg {@code ksync3 version 1.8.9 (13-Feb-2026)}.
 *
 * Both values come from the jar manifest, which the build fills in from the pom, so neither can
 * drift. The date is stored as UTC and formatted here rather than by Maven, because Maven would
 * format it in the building machine's locale.
 */
public class VersionProvider implements IVersionProvider {

    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH);

    @Override
    public String[] getVersion() {
        String built = buildDate();
        return new String[]{"ksync3 version " + version() + (built == null ? "" : " (" + built + ")")};
    }

    /** "development build" when running from classes rather than the jar, as an IDE does. */
    static String version() {
        String version = attribute("Implementation-Version");
        return version == null ? "development build" : version;
    }

    /** Null when there is no manifest, or its date is not one we wrote. */
    static String buildDate() {
        String raw = attribute("Build-Date");
        if (raw == null) {
            return null;
        }
        try {
            return DAY.format(Instant.parse(raw).atZone(ZoneOffset.UTC));
        } catch (RuntimeException ex) {
            return null;
        }
    }

    /** Reads our own jar's manifest, rather than whichever one is first on the classpath. */
    private static String attribute(String name) {
        try {
            String path = VersionProvider.class.getName().replace('.', '/') + ".class";
            URL url = VersionProvider.class.getClassLoader().getResource(path);
            if (url == null || !"jar".equals(url.getProtocol())) {
                return null;
            }
            Manifest manifest = ((JarURLConnection) url.openConnection()).getManifest();
            if (manifest == null) {
                return null;
            }
            Attributes.Name key = new Attributes.Name(name);
            Object value = manifest.getMainAttributes().get(key);
            return value == null ? null : value.toString();
        } catch (Exception ex) {
            return null; // a version string is never worth failing a command over
        }
    }
}
