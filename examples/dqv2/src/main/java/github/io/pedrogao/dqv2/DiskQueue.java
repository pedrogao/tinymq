package github.io.pedrogao.dqv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Iterator;

public class DiskQueue extends AbstractQueue<byte[]> {

    private final Logger log = LoggerFactory.getLogger(DiskQueue.class);

    private final QueueFile queueFile;

    public DiskQueue(String path) throws IOException {
        this.queueFile = new QueueFile(path);
    }

    public DiskQueue(File file) throws IOException {
        this.queueFile = new QueueFile(file, false);
    }

    public DiskQueue(File file, boolean create) throws IOException {
        this.queueFile = new QueueFile(file, create);
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException("iterator unsupported");
    }

    @Override
    public int size() {
        return (int) queueFile.size();
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
        return queueFile.read();
    }

    @Override
    public byte[] peek() {
        return queueFile.peek();
    }
}
