package github.io.pedrogao.diskqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BigQueueImplTest {
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "bigqueue", "unit").toString();
    private IBigQueue bigQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            bigQueue = new BigQueueImpl(testDir, "simple_test");
            assertNotNull(bigQueue);

            for (int j = 1; j <= 3; j++) {
                assertEquals(0L, bigQueue.size());
                assertTrue(bigQueue.isEmpty());

                assertNull(bigQueue.dequeue());
                assertNull(bigQueue.peek());


                bigQueue.enqueue("hello".getBytes());
                assertEquals(1L, bigQueue.size());
                assertFalse(bigQueue.isEmpty());
                assertEquals("hello", new String(bigQueue.peek()));
                assertEquals("hello", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

                bigQueue.enqueue("world".getBytes());
                bigQueue.flush();
                assertEquals(1L, bigQueue.size());
                assertFalse(bigQueue.isEmpty());
                assertEquals("world", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

            }

            bigQueue.close();

        }
    }


    @AfterEach
    public void clean() throws IOException {
        if (bigQueue != null) {
            bigQueue.removeAll();
        }
    }

}