package co.kademi.sync.status;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * An immutable snapshot of what ksync is doing, and the JSON shape written to the status file.
 *
 * The JSON is a contract with whatever is reading it - a status bar, an editor extension, a shell
 * prompt - so the field names are stable and the object is flat. Readers are expected to fall
 * back on `state` alone and ignore fields they do not know.
 */
public class SyncStatus {

    /**
     * The same timestamp shape -logformat ts writes, so a reader can line a status change up
     * against the log entries around it.
     */
    private static final DateTimeFormatter ISO_UTC
            = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final SyncState state;
    private final String detail;
    private final String command;
    private final String localDir;
    private final String url;
    private final String localHash;
    private final String remoteHash;
    private final String lastError;
    private final int errorCount;
    private final Instant updated;

    public static SyncStatus initial(String command, String localDir, String url) {
        return new SyncStatus(SyncState.STARTING, null, command, localDir, url, null, null, null, 0, Instant.now());
    }

    private SyncStatus(SyncState state, String detail, String command, String localDir, String url,
            String localHash, String remoteHash, String lastError, int errorCount, Instant updated) {
        this.state = state;
        this.detail = detail;
        this.command = command;
        this.localDir = localDir;
        this.url = url;
        this.localHash = localHash;
        this.remoteHash = remoteHash;
        this.lastError = lastError;
        this.errorCount = errorCount;
        this.updated = updated;
    }

    public SyncStatus withState(SyncState newState, String newDetail) {
        return new SyncStatus(newState, newDetail, command, localDir, url, localHash, remoteHash,
                lastError, errorCount, Instant.now());
    }

    public SyncStatus withHashes(String newLocalHash, String newRemoteHash) {
        return new SyncStatus(state, detail, command, localDir, url,
                newLocalHash == null ? localHash : newLocalHash,
                newRemoteHash == null ? remoteHash : newRemoteHash,
                lastError, errorCount, Instant.now());
    }

    /**
     * Records a failure. The message is kept even after recovery, because "it is fine now, and
     * here is what went wrong earlier" is more use than a status that forgets.
     */
    public SyncStatus withError(SyncState newState, String message) {
        return new SyncStatus(newState, message, command, localDir, url, localHash, remoteHash,
                message, errorCount + 1, Instant.now());
    }

    public SyncStatus withErrorCount(int count) {
        return new SyncStatus(state, detail, command, localDir, url, localHash, remoteHash,
                lastError, count, Instant.now());
    }

    public SyncState getState() {
        return state;
    }

    public String getDetail() {
        return detail;
    }

    public int getErrorCount() {
        return errorCount;
    }

    /** The state label, with the detail appended when there is one. For a tooltip. */
    public String summary() {
        if (detail == null || detail.isEmpty()) {
            return state.getLabel();
        }
        return state.getLabel() + " - " + detail;
    }

    /**
     * Hand rolled rather than built with net.sf.json, which is the dead dependency the remaining
     * dependabot alert is about (see kademi-dev#24437). A flat object of strings and ints does not
     * justify widening the surface that has to be migrated off it later.
     */
    public String toJson() {
        List<String> fields = new ArrayList<>();
        fields.add(field("state", quote(state.name())));
        fields.add(field("label", quote(state.getLabel())));
        fields.add(field("busy", Boolean.toString(state.isBusy())));
        fields.add(field("problem", Boolean.toString(state.isProblem())));
        fields.add(field("detail", quote(detail)));
        fields.add(field("command", quote(command)));
        fields.add(field("localDir", quote(localDir)));
        fields.add(field("url", quote(url)));
        fields.add(field("localHash", quote(localHash)));
        fields.add(field("remoteHash", quote(remoteHash)));
        fields.add(field("lastError", quote(lastError)));
        fields.add(field("errorCount", Integer.toString(errorCount)));
        fields.add(field("pid", Long.toString(ProcessHandle.current().pid())));
        fields.add(field("updated", quote(ISO_UTC.format(updated))));
        return "{\n" + String.join(",\n", fields) + "\n}\n";
    }

    private static String field(String name, String encodedValue) {
        return "  " + quote(name) + ": " + encodedValue;
    }

    /** @return the JSON encoding of the string, or the literal null when it is absent */
    private static String quote(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 16);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                default:
                    // everything below a space has to be escaped, and a lone one would otherwise
                    // end up raw in the file and break the reader's parser
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
