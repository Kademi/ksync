package io.milton.sync.triplets;

import io.milton.common.Path;
import io.milton.sync.ConflictResolver;
import io.milton.sync.ConflictResolver.ConflictChoice;
import java.io.File;
import org.hashsplit4j.store.MemoryBlobStore;
import org.hashsplit4j.store.MemoryHashStore;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/** A conflict nobody answered must not count as settled, or pull records the remote and the next push overwrites it. */
public class UnresolvedConflictTest {

    private static int unresolvedAfter(ConflictChoice... answers) {
        int[] next = {0};
        ConflictResolver resolver = new ConflictResolver() {
            @Override
            public ConflictChoice showConflictResolver(String message, Integer defaultSecs) {
                return answers[next[0]++];
            }

            @Override
            public Integer getRememberSecs() {
                return null;
            }
        };
        FileUpdatingMergingDeltaListener l = new FileUpdatingMergingDeltaListener(new File("."), new MemoryHashStore(), new MemoryBlobStore(), resolver);
        for (int i = 0; i < answers.length; i++) {
            l.doConflict(Path.root, new DeltaGeneratorDeltasTest.T("f" + i, "h", "f"));
        }
        return l.getUnresolved();
    }

    @Test
    public void onlyNothingIsUnresolved() {
        assertEquals(0, unresolvedAfter(ConflictChoice.LOCAL));
        assertEquals(2, unresolvedAfter(ConflictChoice.NOTHING, ConflictChoice.LOCAL, ConflictChoice.NOTHING));
    }
}
