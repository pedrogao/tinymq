package github.io.pedrogao.dqv1;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class DiskQueueTest {

    @Test
    void size() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        DiskQueue queue = new DiskQueue(file, true);
        int size = queue.size();
        assertEquals(size, 0);

        queue.offer("Hello Queue".getBytes());
        size = queue.size();
        assertEquals(size, 1);
    }

    @Test
    void offer() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        DiskQueue queue = new DiskQueue(file, true);

        for (int i = 0; i < 1000; i++) {
            queue.offer(("Hello Queue" + i).getBytes());
        }
        int size = queue.size();
        assertEquals(size, 1000);

        for (int i = 0; i < 1000; i++) {
            byte[] peek = queue.peek();
            assertEquals(peek.length, ("Hello Queue" + 0).getBytes().length);
        }
        size = queue.size();
        assertEquals(size, 1000);

        for (int i = 0; i < 1000; i++) {
            byte[] poll = queue.poll();
            assertEquals(poll.length, ("Hello Queue" + i).getBytes().length);
        }
        size = queue.size();
        assertEquals(size, 0);
    }
}