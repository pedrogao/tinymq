package github.io.pedrogao.diskqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BigArrayImplTest {
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "bigarray", "unit").toString();
    private IBigArray bigArray;

    @Test
    public void simpleTest() throws IOException {
        bigArray = new BigArrayImpl(testDir, "simple_test");
        assertNotNull(bigArray);

        for (int i = 1; i <= 3; i++) {
            assertEquals(0L, bigArray.getTailIndex());
            assertEquals(0L, bigArray.getHeadIndex());
            assertEquals(0L, bigArray.size());
            assertTrue(bigArray.isEmpty());
            assertFalse(bigArray.isFull());
            try {
                bigArray.get(0);
                fail("IndexOutOfBoundsException should be thrown here");
            } catch (IndexOutOfBoundsException ignored) {
            }
            try {
                bigArray.get(1);
                fail("IndexOutOfBoundsException should be thrown here");
            } catch (IndexOutOfBoundsException ignored) {
            }
            try {
                bigArray.get(Long.MAX_VALUE);
                fail("IndexOutOfBoundsException should be thrown here");
            } catch (IndexOutOfBoundsException ignored) {
            }

            bigArray.append("hello".getBytes());
            assertEquals(0L, bigArray.getTailIndex());
            assertEquals(1L, bigArray.getHeadIndex());
            assertEquals(1L, bigArray.size());
            assertFalse(bigArray.isEmpty());
            assertFalse(bigArray.isFull());
            assertEquals("hello", new String(bigArray.get(0)));

            bigArray.flush();

            bigArray.append("world".getBytes());
            assertEquals(0L, bigArray.getTailIndex());
            assertEquals(2L, bigArray.getHeadIndex());
            assertEquals(2L, bigArray.size());
            assertFalse(bigArray.isEmpty());
            assertFalse(bigArray.isFull());
            assertEquals("hello", new String(bigArray.get(0)));
            assertEquals("world", new String(bigArray.get(1)));

            bigArray.removeAll();
        }
    }

    @AfterEach
    public void clean() throws IOException {
        if (bigArray != null) {
            bigArray.removeAll();
        }
    }
}