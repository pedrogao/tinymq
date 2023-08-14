package github.io.pedrogao.dqv1;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class QueueFileTest {

    @Test
    void read() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);
    }

    @Test
    void peek() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.peek();
        assertNull(read);

        queueFile.write("Hello Queue".getBytes());

        for (int i = 0; i < 10; i++) {
            read = queueFile.peek();
            assertEquals(read.length, "Hello Queue".getBytes().length);
        }

        read = queueFile.read();
        assertEquals(read.length, "Hello Queue".getBytes().length);

        read = queueFile.peek();
        assertNull(read);
    }

    @Test
    void write() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);

        queueFile.write("Hello Queue".getBytes());
        read = queueFile.read();
        assertEquals(read.length, "Hello Queue".getBytes().length);
    }

    @Test
    void writeToMany() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);

        for (int i = 0; i < 100; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }

        for (int i = 0; i < 100; i++) {
            read = queueFile.read();
            assertEquals(read.length, ("Hello Queue" + i).getBytes().length);
        }
    }


    @Test
    void writeAfterClose() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);

        for (int i = 0; i < 100; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }

        queueFile.close();

        queueFile = new QueueFile(file);
        for (int i = 0; i < 100; i++) {
            read = queueFile.read();
            assertEquals(read.length, ("Hello Queue" + i).getBytes().length);
        }
    }

    @Test
    void concurrentWriteToMany() throws IOException, InterruptedException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);

        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                queueFile.write("Hello Queue".getBytes());
            }
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                queueFile.write("Hello Queue".getBytes());
            }
        });

        // start threads
        thread1.start();
        thread2.start();

        // join main thread
        thread1.join();
        thread2.join();

        assertEquals(queueFile.size(), 2000);

        for (int i = 0; i < 2000; i++) {
            read = queueFile.read();
            assertEquals(read.length, "Hello Queue".getBytes().length);
        }
    }

    @Test
    void close() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        QueueFile queueFile = new QueueFile(file, true);
        byte[] read = queueFile.read();
        assertNull(read);
        assertFalse(queueFile.isClosed());
        queueFile.close();
        assertTrue(queueFile.isClosed());
        read = queueFile.read();
        assertNull(read);
    }
}