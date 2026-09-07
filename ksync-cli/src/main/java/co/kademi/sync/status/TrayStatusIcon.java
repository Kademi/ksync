package co.kademi.sync.status;

import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An icon in the OS status bar - the Windows notification area, the macOS menu bar, a Linux panel.
 *
 * java.awt.SystemTray is part of the JDK, so this costs no dependency. It is not available
 * everywhere, and where it is unavailable it says so rather than failing later, so
 * {@link #createOrNull} returns null and the sync carries on with the status file alone.
 *
 * Known platform limits, all handled here or in {@link StatusIcon}:
 * <ul>
 * <li>Linux: AWT speaks only the older XEmbed tray protocol. Ubuntu's appindicators extension
 * hosts it, so this works on a stock Ubuntu desktop; a GNOME without that extension reports no
 * tray and gets nothing. Transparency is not composited, which {@link StatusIcon} paints around.
 * <li>macOS: needs {@link #configureMacOsAccessoryMode} before AWT starts, or the process takes a
 * Dock icon and steals focus.
 * <li>Windows: the icon may start life in the hidden overflow flyout, which is the user's setting
 * to change and nothing we can do from here.
 * </ul>
 */
public class TrayStatusIcon implements StatusSink {

    private static final Logger log = LoggerFactory.getLogger(TrayStatusIcon.class);

    /**
     * Makes the process a macOS accessory: no Dock icon, no focus stealing, just the menu bar
     * item. Must be called before anything touches AWT, because the property is read when the
     * application object is created.
     *
     * @param dialogsPossible whether a conflict may still be put on screen as a dialog. An
     * accessory application has no Dock icon to click, so a dialog from one can end up behind
     * another window with no way to reach it - a sync that hangs on a question nobody can see.
     * That is strictly worse than a stray Dock icon, so when a dialog is still possible this
     * leaves the property alone and accepts the icon.
     */
    public static void configureMacOsAccessoryMode(boolean dialogsPossible) {
        if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac")) {
            return;
        }
        if (dialogsPossible) {
            log.debug("Not asking for macOS accessory mode: a conflict dialog is still possible and would be unreachable without a Dock icon");
            return;
        }
        System.setProperty("apple.awt.UIElement", "true");
        log.debug("Running as a macOS accessory, so the status icon appears without a Dock icon");
    }

    /**
     * @return the icon, or null when this platform has no status bar we can reach, which is not an
     * error worth reporting loudly
     */
    public static TrayStatusIcon createOrNull() {
        try {
            if (!SystemTray.isSupported()) {
                log.debug("No system tray on this platform, so no status icon");
                return null;
            }
            return new TrayStatusIcon();
        } catch (Throwable e) {
            // HeadlessException, an AWTError from a mac with no window session, a linux box with
            // no display. None of them are worth more than a debug line
            log.debug("Could not create the status icon", e);
            return null;
        }
    }

    /**
     * How much bigger than the reported size to draw, where we paint our own background.
     *
     * getTrayIconSize is a hint, not the truth. Measured on Ubuntu GNOME it reports 24 while the
     * panel allocates about 36, and the shortfall shows as a light grey L down the right and
     * bottom edges - the uncovered part of the icon window. Overshooting fills the slot, and the
     * failure mode if a panel is smaller than we drew is a flat colour field cropped at the
     * edges, which is invisible. Setting imageAutoSize does not help: it scales to the reported
     * size, which is the size that was too small to begin with.
     */
    private static final double OVERSIZE = 1.5;

    private final SystemTray tray;
    private final TrayIcon icon;
    private final MenuItem summaryItem;
    private final int size;
    private SyncState current;
    private boolean closed;

    private TrayStatusIcon() throws Exception {
        tray = SystemTray.getSystemTray();
        int reported = Math.max(16, tray.getTrayIconSize().width);
        // Only where we paint an opaque background, which is the case the grey edge shows up in.
        // Windows and macOS composite transparency properly and are honest about the size.
        size = StatusIcon.fillsBackground() ? (int) Math.round(reported * OVERSIZE) : reported;

        summaryItem = new MenuItem(SyncState.STARTING.getLabel());
        summaryItem.setEnabled(false); // a label, not an action
        MenuItem quit = new MenuItem("Quit ksync");
        quit.addActionListener(e -> {
            log.info("Quitting, asked from the status icon");
            System.exit(0);
        });
        PopupMenu menu = new PopupMenu();
        menu.add(summaryItem);
        menu.addSeparator();
        menu.add(quit);

        icon = new TrayIcon(StatusIcon.create(SyncState.STARTING, size), "ksync", menu);
        // Left off where we oversized deliberately, because autoSize would scale it straight
        // back down to the reported size and put the grey edge back.
        icon.setImageAutoSize(!StatusIcon.fillsBackground());
        current = SyncState.STARTING;
        tray.add(icon);
        log.debug("Status icon added to the system tray, drawn at {}px for a reported {}px slot", size, reported);
    }

    @Override
    public synchronized void report(SyncStatus status) {
        if (closed) {
            return;
        }
        try {
            if (status.getState() != current) {
                icon.setImage(StatusIcon.create(status.getState(), size));
                current = status.getState();
            }
            // Windows caps the tooltip and drops anything longer, so keep it short
            icon.setToolTip(trim("ksync: " + status.summary(), 120));
            summaryItem.setLabel(trim(status.summary(), 60));
        } catch (RuntimeException ex) {
            log.debug("Could not update the status icon", ex);
        }
    }

    @Override
    public synchronized boolean alert(String title, String body, boolean problem) {
        if (closed) {
            return false;
        }
        // On macOS the tray balloon has never been dependable, so go straight to the OS
        if (Notifier.isMac()) {
            return Notifier.send(title, body, problem);
        }
        try {
            icon.displayMessage(title, body, problem ? TrayIcon.MessageType.ERROR : TrayIcon.MessageType.INFO);
            return true;
        } catch (RuntimeException ex) {
            log.debug("Could not show a tray message, trying the OS instead", ex);
            return Notifier.send(title, body, problem);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            tray.remove(icon);
        } catch (RuntimeException ex) {
            log.debug("Could not remove the status icon", ex);
        }
    }

    private static String trim(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }
}
