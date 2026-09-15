package co.kademi.sync;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * The keyboard way out of a sync, for the terminals where ctrl-c does not arrive.
 *
 * What matters is that it is hard to miss and impossible to trip over: the words someone would
 * actually type all work, and nothing else ends a sync that was meant to keep running.
 */
public class StopKeyTest {

    /** One call per line, then null for end of input, like a console does */
    private static java.util.function.Supplier<String> typed(String... lines) {
        List<String> list = Arrays.asList(lines);
        Iterator<String> it = list.iterator();
        return () -> it.hasNext() ? it.next() : null;
    }

    @Test
    public void theWordsSomeoneWouldTry_allStopIt() {
        for (String word : new String[]{"q", "Q", "quit", "QUIT", "exit", "stop", " q ", "Stop"}) {
            assertTrue(word, StopKey.isStop(word));
        }
    }

    @Test
    public void anythingElse_doesNot() {
        for (String word : new String[]{"", " ", "qq", "queue", "quitter", "s", "no", "exits"}) {
            assertFalse(word, StopKey.isStop(word));
        }
    }

    @Test
    public void nullIsNotAStopWord() {
        // it means end of input, which is handled where the reading is done, not here
        assertFalse(StopKey.isStop(null));
    }

    @Test
    public void typingQ_ends() {
        assertEquals(0, StopKey.readUntilStopped(typed("q")));
    }

    /** Enter on its own, and anything else, leaves the sync running - the reader asks again */
    @Test
    public void otherLines_areNotAWayOut() {
        assertEquals(0, StopKey.readUntilStopped(typed("", "hello", "  ", "quit")));
    }

    /** ctrl-d, or the terminal going away: nobody is left to ask, so it stops rather than hanging */
    @Test
    public void endOfInput_ends() {
        assertEquals(0, StopKey.readUntilStopped(typed()));
    }
}
