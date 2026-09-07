package co.kademi.sync.status;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.RenderingHints;
import java.awt.geom.Ellipse2D;
import java.awt.image.BaseMultiResolutionImage;
import java.awt.image.BufferedImage;
import java.util.Locale;

/**
 * Draws the status bar icon.
 *
 * Drawn rather than shipped as image files, because the icon has to work at whatever size the
 * platform asks for - 16px in the Windows notification area, 22 or 24 in a Linux panel, and a
 * retina macOS menu bar wanting twice what it reports.
 *
 * Colour alone would be a poor signal: it fails for a colourblind reader and it is invisible at a
 * glance on a busy panel. So each state also gets a distinct white glyph, and the two agree.
 */
public class StatusIcon {

    /** Green: working, nothing to do */
    private static final Color OK = new Color(0x2E9E4F);
    /** Blue: talking to the server */
    private static final Color BUSY = new Color(0x1E7FD6);
    /** Amber: starting up, not yet known to be healthy */
    private static final Color WAITING = new Color(0xE0A030);
    /** Red: needs a person */
    private static final Color PROBLEM = new Color(0xD03030);
    /** Grey: not running */
    private static final Color INACTIVE = new Color(0x8C8C8C);

    private StatusIcon() {
    }

    /**
     * An image for the tray, with a 2x variant so a retina macOS menu bar has something crisp to
     * scale from rather than blowing up the 1x.
     */
    public static Image create(SyncState state, int size) {
        return new BaseMultiResolutionImage(
                render(state, size, fillsBackground()),
                render(state, size * 2, fillsBackground()));
    }

    /**
     * Whether the icon needs to paint its own opaque background.
     *
     * On X11 a tray icon is not composited against the panel: transparent pixels come out as the
     * default widget background, which is a light grey square around the glyph and looks broken on
     * a dark top bar. Verified on Ubuntu GNOME. Painting the full square avoids it. Windows and
     * macOS composite properly, so there the icon keeps its transparent corners.
     */
    static boolean fillsBackground() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return !os.contains("win") && !os.contains("mac");
    }

    static BufferedImage render(SyncState state, int size, boolean fillBackground) {
        BufferedImage img = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_PURE);
            Color colour = colourFor(state);
            g.setColor(colour);
            if (fillBackground) {
                g.fillRect(0, 0, size, size);
            } else {
                // inset by a pixel so the disc does not touch the edge of a menu bar item
                double inset = size / 16.0;
                g.fill(new Ellipse2D.Double(inset, inset, size - inset * 2, size - inset * 2));
            }
            g.setColor(Color.WHITE);
            glyph(g, state, size);
        } finally {
            g.dispose();
        }
        return img;
    }

    static Color colourFor(SyncState state) {
        switch (state) {
            case IDLE:
                return OK;
            case PUSHING:
            case PULLING:
                return BUSY;
            case STARTING:
            case SCANNING:
                return WAITING;
            case BLOCKED:
            case OFFLINE:
            case FAILED:
                return PROBLEM;
            default:
                return INACTIVE;
        }
    }

    /**
     * A shape per state, so the icon is readable without relying on the colour. Coordinates are
     * fractions of the icon size, so they hold at every size the platform asks for.
     */
    private static void glyph(Graphics2D g, SyncState state, int size) {
        double c = size / 2.0;
        double u = size / 8.0; // one eighth, the unit the shapes are built from
        switch (state) {
            case PUSHING:
                triangle(g, c, c, u * 2, true);
                break;
            case PULLING:
                triangle(g, c, c, u * 2, false);
                break;
            case IDLE:
                // a tick
                g.setStroke(new java.awt.BasicStroke((float) (u * 0.9), java.awt.BasicStroke.CAP_ROUND, java.awt.BasicStroke.JOIN_ROUND));
                g.drawPolyline(
                        new int[]{(int) (c - u * 1.6), (int) (c - u * 0.4), (int) (c + u * 1.7)},
                        new int[]{(int) c, (int) (c + u * 1.2), (int) (c - u * 1.3)}, 3);
                break;
            case STARTING:
            case SCANNING:
                // three dots, the universal "working on it"
                for (int i = -1; i <= 1; i++) {
                    double d = u * 0.9;
                    g.fill(new Ellipse2D.Double(c + i * u * 1.9 - d / 2, c - d / 2, d, d));
                }
                break;
            case BLOCKED:
                // two arrows pointing apart: both sides moved, which is what being blocked means.
                // Deliberately not the exclamation FAILED uses - a status bar should tell "you
                // need to pull" apart from "something broke", because the fix is different.
                triangle(g, c - u * 1.7, c, u * 1.5, true);
                triangle(g, c + u * 1.7, c, u * 1.5, false);
                break;
            case FAILED:
                // exclamation mark
                g.fillRoundRect((int) (c - u * 0.45), (int) (c - u * 2.2), (int) (u * 0.9), (int) (u * 2.9), (int) u, (int) u);
                g.fill(new Ellipse2D.Double(c - u * 0.55, c + u * 1.4, u * 1.1, u * 1.1));
                break;
            case OFFLINE:
                // a bar, as in "no connection"
                g.fillRoundRect((int) (c - u * 2.2), (int) (c - u * 0.45), (int) (u * 4.4), (int) (u * 0.9), (int) u, (int) u);
                break;
            default:
                // stopped: a square
                g.fillRect((int) (c - u * 1.5), (int) (c - u * 1.5), (int) (u * 3), (int) (u * 3));
                break;
        }
    }

    private static void triangle(Graphics2D g, double cx, double cy, double r, boolean up) {
        int[] xs = new int[]{(int) (cx - r), (int) (cx + r), (int) cx};
        int[] ys = up
                ? new int[]{(int) (cy + r * 0.8), (int) (cy + r * 0.8), (int) (cy - r * 0.9)}
                : new int[]{(int) (cy - r * 0.8), (int) (cy - r * 0.8), (int) (cy + r * 0.9)};
        g.fillPolygon(xs, ys, 3);
    }
}
