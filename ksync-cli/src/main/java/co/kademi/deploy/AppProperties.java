package co.kademi.deploy;

/**
 *
 * @author dylan
 */
public class AppProperties {

    private final String version;
    private final String clusters;

    public AppProperties(String version, String clusters) {
        this.version = version;
        this.clusters = clusters;
    }

    public String getVersion() {
        return version;
    }

    public String getClusters() {
        return clusters;
    }

}
