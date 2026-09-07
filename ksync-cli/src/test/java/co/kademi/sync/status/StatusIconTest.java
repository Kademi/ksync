package co.kademi.sync.status;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * The icon is drawn rather than shipped, so these check it actually draws something at the sizes
 * the platforms ask for, and that the states are told apart by shape and not by colour alone.
 */
public class StatusIconTest {

    @Test
    public void rendersAtEverySizeAPlatformAsksFor() {
        for (int size : new int[]{16, 22, 24, 32, 44, 48}) {
            for (SyncState state : SyncState.values()) {
                BufferedImage img = StatusIcon.render(state, size, false);
                assertEquals(size, img.getWidth());
                assertEquals(size, img.getHeight());
                assertTrue(state + " at " + size + " drew nothing", hasInk(img));
            }
        }
    }

    /**
     * On X11 a tray icon is not composited against the panel, so transparent pixels come out as a
     * light grey square. With the background filled there must be no transparency left at all.
     */
    @Test
    public void theFilledBackgroundIsFullyOpaque() {
        BufferedImage img = StatusIcon.render(SyncState.IDLE, 24, true);
        for (int x = 0; x < img.getWidth(); x++) {
            for (int y = 0; y < img.getHeight(); y++) {
                int alpha = (img.getRGB(x, y) >>> 24) & 0xff;
                assertEquals("transparent pixel at " + x + "," + y + " would show as grey on a panel",
                        255, alpha);
            }
        }
    }

    /** Without the fill the corners stay clear, so a menu bar item is not a solid block. */
    @Test
    public void theUnfilledIconKeepsTransparentCorners() {
        BufferedImage img = StatusIcon.render(SyncState.IDLE, 24, false);
        assertEquals("corner should be transparent", 0, (img.getRGB(0, 0) >>> 24) & 0xff);
    }

    /**
     * Colour alone is not a signal a colourblind reader can use, and at 16px on a busy panel it
     * is not much of one for anybody. So the states that share a colour must differ in shape -
     * that is what this checks, with the colour held constant to isolate the glyph.
     */
    @Test
    public void statesSharingAColourDifferInShape() {
        // the three reds. Blocked is the one worth care: "you need to pull" and "something broke"
        // want different responses, so they must not look the same
        assertDifferentShapes(SyncState.BLOCKED, SyncState.FAILED);
        assertDifferentShapes(SyncState.BLOCKED, SyncState.OFFLINE);
        assertDifferentShapes(SyncState.OFFLINE, SyncState.FAILED);
        // the two blues
        assertDifferentShapes(SyncState.PUSHING, SyncState.PULLING);
        // and the resting states
        assertDifferentShapes(SyncState.IDLE, SyncState.STOPPED);
    }

    /**
     * Starting and scanning are deliberately the same: same colour, same "getting going, nothing
     * known yet" meaning, and telling them apart in a 16px icon would buy the reader nothing.
     */
    @Test
    public void startingAndScanningShareAGlyphOnPurpose() {
        assertEquals(shapeOf(StatusIcon.render(SyncState.STARTING, 32, true)),
                shapeOf(StatusIcon.render(SyncState.SCANNING, 32, true)));
    }

    /** Every other pair should be distinct, which is 8 glyphs across the 9 states. */
    @Test
    public void thereIsAGlyphPerMeaning() {
        Set<String> shapes = new HashSet<>();
        for (SyncState state : SyncState.values()) {
            shapes.add(shapeOf(StatusIcon.render(state, 32, true)));
        }
        assertEquals("one shared pair (starting/scanning) and the rest distinct",
                SyncState.values().length - 1, shapes.size());
    }

    private static void assertDifferentShapes(SyncState a, SyncState b) {
        assertFalse(a + " and " + b + " must not look the same",
                shapeOf(StatusIcon.render(a, 32, true)).equals(shapeOf(StatusIcon.render(b, 32, true))));
    }

    @Test
    public void aProblemIsRedAndHealthyIsNot() {
        Color problem = StatusIcon.colourFor(SyncState.FAILED);
        assertEquals(problem, StatusIcon.colourFor(SyncState.BLOCKED));
        assertEquals(problem, StatusIcon.colourFor(SyncState.OFFLINE));
        assertTrue("a problem should read as red", problem.getRed() > problem.getGreen() + 60);
        Color ok = StatusIcon.colourFor(SyncState.IDLE);
        assertTrue("healthy should read as green", ok.getGreen() > ok.getRed() + 40);
        assertFalse(problem.equals(ok));
    }

    @Test
    public void multiResolutionImageIsProducedForRetina() {
        assertNotNull(StatusIcon.create(SyncState.IDLE, 22));
    }

    /** A crude signature of where the white glyph pixels are, independent of the base colour. */
    private static String shapeOf(BufferedImage img) {
        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < img.getHeight(); y += 2) {
            for (int x = 0; x < img.getWidth(); x += 2) {
                int rgb = img.getRGB(x, y);
                int r = (rgb >> 16) & 0xff, g = (rgb >> 8) & 0xff, b = rgb & 0xff;
                sb.append(r > 200 && g > 200 && b > 200 ? '#' : '.');
            }
        }
        return sb.toString();
    }

    private static boolean hasInk(BufferedImage img) {
        for (int x = 0; x < img.getWidth(); x++) {
            for (int y = 0; y < img.getHeight(); y++) {
                if (((img.getRGB(x, y) >>> 24) & 0xff) != 0) {
                    return true;
                }
            }
        }
        return false;
    }
}
