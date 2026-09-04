package io.milton.sync;

import java.awt.BorderLayout;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.FlowLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A desktop dialog asking how to resolve a conflict. See {@link ConflictResolvers} for when it
 * is used.
 *
 * Swing rather than JavaFX deliberately: it is part of the jdk, so it costs nothing, where
 * OpenJFX is a 26MB platform-classified dependency that would more than double the shaded jar
 * or force a build per platform. Everything a conflict dialog needs is here. Swapping to JavaFX
 * later means writing one more ConflictResolver and changing nothing else.
 */
public class GuiConflictResolver implements ConflictResolver {

    private static final Logger log = LoggerFactory.getLogger(GuiConflictResolver.class);

    private Integer rememberSecs;

    @Override
    public Integer getRememberSecs() {
        return rememberSecs;
    }

    /**
     * Synchronized because sync runs on background threads, and two dialogs racing each other
     * is worse than waiting.
     */
    @Override
    public synchronized ConflictChoice showConflictResolver(String message, Integer defaultSecs) {
        final ConflictChoice[] choice = {ConflictChoice.NOTHING};
        try {
            SwingUtilities.invokeAndWait(() -> choice[0] = ask(message, defaultSecs));
        } catch (Exception ex) {
            // A dialog that cannot be shown must not take the sync down with it
            log.warn("Could not show the conflict dialog, leaving both files unchanged", ex);
            return ConflictChoice.NOTHING;
        }
        return choice[0];
    }

    /**
     * Asks the window manager to bring this process to the front. Needed on macOS, where a
     * process launched from a terminal is not the active application and its windows open
     * behind whatever is. Harmless everywhere else.
     */
    private static void requestForeground() {
        try {
            if (Desktop.isDesktopSupported()
                    && Desktop.getDesktop().isSupported(Desktop.Action.APP_REQUEST_FOREGROUND)) {
                Desktop.getDesktop().requestForeground(true);
            }
        } catch (Throwable e) {
            log.debug("Could not bring the dialog to the front", e);
        }
    }

    /** File paths can contain characters html would eat, and newlines need to survive. */
    static String escapeHtml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\n", "<br>");
    }

    private ConflictChoice ask(String message, Integer defaultSecs) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
            // the cross platform look and feel is fine
        }

        // An html label wraps itself at a fixed width. A JTextArea with a forced preferred
        // size fights JOptionPane's own sizing, which is what clipped the buttons.
        JPanel panel = new JPanel(new BorderLayout(0, 14));
        panel.add(new JLabel("<html><body style='width:430px'>"
                + escapeHtml(message) + "</body></html>"), BorderLayout.NORTH);

        JPanel rememberRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        rememberRow.add(new JLabel("Remember this choice for"));
        JSpinner spinner = new JSpinner(new SpinnerNumberModel(
                defaultSecs == null ? 0 : Math.max(0, defaultSecs), 0, 86400, 10));
        spinner.setPreferredSize(new Dimension(78, spinner.getPreferredSize().height));
        rememberRow.add(spinner);
        rememberRow.add(new JLabel("seconds"));
        panel.add(rememberRow, BorderLayout.CENTER);

        String[] options = {"Keep local", "Take remote", "Do nothing"};
        JOptionPane pane = new JOptionPane(panel, JOptionPane.WARNING_MESSAGE,
                JOptionPane.DEFAULT_OPTION, null, options, options[0]);
        // No owner window. An undecorated JFrame parent is invisible enough on linux, but on
        // macOS it is a real window: it takes a dock icon and can flash on screen.
        JDialog dialog = pane.createDialog(null, "ksync - file conflict");
        dialog.setAlwaysOnTop(true);
        dialog.setLocationRelativeTo(null);
        try {
            // A CLI process is not the foreground app, and macOS will not let its window take
            // focus by itself, so the dialog opens behind the terminal and reads as a hang.
            // requestForeground is the supported way to ask; it is a no-op where unsupported.
            requestForeground();
            dialog.setVisible(true);

            Integer secs = (Integer) spinner.getValue();
            rememberSecs = secs == null || secs <= 0 ? null : secs;
            Object picked = pane.getValue();
            // closing the dialog leaves it at NOTHING, which changes neither file
            if (options[0].equals(picked)) {
                return ConflictChoice.LOCAL;
            } else if (options[1].equals(picked)) {
                return ConflictChoice.REMOTE;
            }
            return ConflictChoice.NOTHING;
        } finally {
            dialog.dispose();
        }
    }
}
