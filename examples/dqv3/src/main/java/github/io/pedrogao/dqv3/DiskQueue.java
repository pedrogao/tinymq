package github.io.pedrogao.dqv3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Iterator;

public class DiskQueue extends AbstractQueue<byte[]> {

    private final Logger log = LoggerFactory.getLogger(DiskQueue.class);

    private final QueueFile queueFile;

    public DiskQueue(String path) throws IOException {
        this.queueFile = new QueueFile(path);
    }

    public DiskQueue(String path, String name) throws IOException {
        this.queueFile = new QueueFile(path, name);
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException("iterator unsupported");
    }

    @Override
    public int size() {
        return (int) queueFile.getSize();
    }

    @Override
    public boolean offer(byte[] bytes) {
        try {
            queueFile.write(bytes);
            return true;
        } catch (Exception e) {
            log.error("offer error", e);
            return false;
        }
    }

    @Override
    public byte[] poll() {
        try {
            return queueFile.read();
        } catch (IOException e) {
            log.error("poll error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] peek() {
        try {
            return queueFile.peek();
        } catch (IOException e) {
            log.error("peek error", e);
            throw new RuntimeException(e);
        }
    }
}
