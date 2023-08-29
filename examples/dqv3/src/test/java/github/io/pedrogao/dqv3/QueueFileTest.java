package github.io.pedrogao.dqv3;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class QueueFileTest {

    private final Random random = new Random(System.currentTimeMillis());

    @Test
    void write() throws IOException, InterruptedException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);

        for (int i = 0; i < 1000; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }
        long size = queueFile.getSize();
        assertEquals(size, 1000);

        Thread thread1 = new Thread(new WriteTask(queueFile, 100));
        Thread thread2 = new Thread(new WriteTask(queueFile, 100));

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        size = queueFile.getSize();
        assertEquals(size, 1200);
    }

    @Test
    void writeRotateFiles() throws IOException, InterruptedException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path, "rotate", 1024);

        for (int i = 0; i < 1000; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }
        long size = queueFile.getSize();
        assertEquals(size, 1000);

        Thread thread1 = new Thread(new WriteTask(queueFile, 100));
        Thread thread2 = new Thread(new WriteTask(queueFile, 100));

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        size = queueFile.getSize();
        assertEquals(size, 1200);
    }

    static class WriteTask implements Runnable {

        private final QueueFile queueFile;

        private final int size;

        WriteTask(QueueFile queueFile, int size) {
            this.queueFile = queueFile;
            this.size = size;
        }

        @Override
        public void run() {
            for (int i = 0; i < size; i++) {
                try {
                    queueFile.write(("Hello Queue" + i).getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    void read() throws IOException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);

        for (int i = 0; i < 1000; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }
        long size = queueFile.getSize();
        assertEquals(size, 1000);

        for (int i = 0; i < 1000; i++) {
            byte[] bytes = queueFile.read();
            assertEquals(new String(bytes), "Hello Queue" + i);
        }
        size = queueFile.getSize();
        assertEquals(size, 0);
    }

    @Test
    void peek() throws IOException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);

        for (int i = 0; i < 1000; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }
        long size = queueFile.getSize();
        assertEquals(size, 1000);

        for (int i = 0; i < 1000; i++) {
            byte[] bytes = queueFile.peek();
            assertEquals(new String(bytes), "Hello Queue" + 0);
        }
        size = queueFile.getSize();
        assertEquals(size, 1000);
    }

    @Test
    void getSize() throws IOException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);
        long size = queueFile.getSize();
        assertEquals(size, 0);

        queueFile.write("Hello Queue1".getBytes());
        size = queueFile.getSize();
        assertEquals(size, 1);

        queueFile.write("Hello Queue2".getBytes());
        size = queueFile.getSize();
        assertEquals(size, 2);

        byte[] bytes = queueFile.read();
        assertEquals(new String(bytes), "Hello Queue1");
        size = queueFile.getSize();
        assertEquals(size, 1);
    }

    @Test
    void getMaxBytesPerFile() throws IOException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);
        assertEquals(queueFile.getMaxBytesPerFile(), QueueFile.DEFAULT_MAX_BYTES_PER_FILE);

        path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        queueFile = new QueueFile(path, "test", 1024);
        assertEquals(queueFile.getMaxBytesPerFile(), 1024);
    }

    @Test
    void getSyncInterval() throws IOException {
        String path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        QueueFile queueFile = new QueueFile(path);
        assertEquals(queueFile.getSyncInterval(), QueueFile.DEFAULT_SYNC_INTERVAL);

        path = Files.createTempDirectory("queue" + random.nextInt()).toString();
        queueFile = new QueueFile(path, "test", 1024, 10);
        assertEquals(queueFile.getSyncInterval(), 10);
    }
}