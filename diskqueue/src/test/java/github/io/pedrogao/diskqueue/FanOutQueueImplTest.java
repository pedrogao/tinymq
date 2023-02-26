package github.io.pedrogao.diskqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FanOutQueueImplTest {
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "foqueue", "unit").toString();
    private IFanOutQueue foQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            foQueue = new FanOutQueueImpl(testDir, "simple_test");
            assertNotNull(foQueue);

            String fid = "simpleTest";
            for (int j = 1; j <= 3; j++) {
                assertEquals(0L, foQueue.size(fid));
                assertTrue(foQueue.isEmpty(fid));

                assertNull(foQueue.dequeue(fid));
                assertNull(foQueue.peek(fid));


                foQueue.enqueue("hello".getBytes());
                assertEquals(1L, foQueue.size(fid));
                assertFalse(foQueue.isEmpty(fid));
                assertEquals("hello", new String(foQueue.peek(fid)));
                assertEquals("hello", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

                foQueue.enqueue("world".getBytes());
                foQueue.flush();
                assertEquals(1L, foQueue.size(fid));
                assertFalse(foQueue.isEmpty(fid));
                assertEquals("world", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

            }

            foQueue.close();
        }
    }

    @AfterEach
    public void clean() throws IOException {
        if (foQueue != null) {
            foQueue.removeAll();
        }
    }
}