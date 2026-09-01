package io.milton.sync;

import io.milton.sync.ConsoleConflictResolver.ConflictChoice;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

public class ConsoleConflictResolverTest {

    private final InputStream realIn = System.in;

    @After
    public void restoreStdin() {
        System.setIn(realIn);
    }

    private ConsoleConflictResolver on(String typed) {
        System.setIn(new ByteArrayInputStream(typed.getBytes(StandardCharsets.UTF_8)));
        return new ConsoleConflictResolver();
    }

    @Test
    public void picksLocalRemoteOrNothing() {
        assertEquals(ConflictChoice.LOCAL, on("l\n0\n").showConflictResolver("m", null));
        assertEquals(ConflictChoice.REMOTE, on("r\n0\n").showConflictResolver("m", null));
        assertEquals(ConflictChoice.NOTHING, on("n\n0\n").showConflictResolver("m", null));
    }

    @Test
    public void acceptsWholeWordsAndAnyCase() {
        assertEquals(ConflictChoice.REMOTE, on("REMOTE\n0\n").showConflictResolver("m", null));
        assertEquals(ConflictChoice.LOCAL, on("  Local  \n0\n").showConflictResolver("m", null));
    }

    @Test
    public void reAsksUntilTheAnswerIsValid() {
        ConsoleConflictResolver r = on("\nwhat?\nx\nr\n0\n");
        assertEquals(ConflictChoice.REMOTE, r.showConflictResolver("m", null));
    }

    @Test
    public void readsRememberSeconds() {
        ConsoleConflictResolver r = on("l\n30\n");
        r.showConflictResolver("m", null);
        assertEquals(Integer.valueOf(30), r.getRememberSecs());
    }

    @Test
    public void blankOrJunkSecondsKeepsTheDefault() {
        ConsoleConflictResolver r = on("l\n\n");
        r.showConflictResolver("m", 5);
        assertEquals(Integer.valueOf(5), r.getRememberSecs());

        r = on("l\nnot a number\n");
        r.showConflictResolver("m", 5);
        assertEquals(Integer.valueOf(5), r.getRememberSecs());

        r = on("l\n-1\n");
        r.showConflictResolver("m", 5);
        assertEquals(Integer.valueOf(5), r.getRememberSecs());
    }

    @Test
    public void noDefaultAndNoAnswerMeansDoNotRemember() {
        ConsoleConflictResolver r = on("l\n\n");
        r.showConflictResolver("m", null);
        assertNull(r.getRememberSecs());
    }

    @Test
    public void nothingToReadLeavesFilesAlone() {
        // non-interactive run: must not silently overwrite anything
        ConsoleConflictResolver r = on("");
        assertEquals(ConflictChoice.NOTHING, r.showConflictResolver("m", null));
    }
}
