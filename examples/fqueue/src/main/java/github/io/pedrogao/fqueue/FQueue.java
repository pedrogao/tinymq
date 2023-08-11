package github.io.pedrogao.fqueue;

import github.io.pedrogao.fqueue.exception.FileFormatException;
import github.io.pedrogao.fqueue.queue.FileQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FQueue extends AbstractQueue<byte[]> implements Serializable {

    private final Logger log = LoggerFactory.getLogger(FQueue.class);

    private final FileQueue fileQueue;

    private final Lock lock = new ReentrantReadWriteLock().writeLock();

    public FQueue(String path) {
        fileQueue = new FileQueue(path);
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException("iterator Unsupported now");
    }

    @Override
    public int size() {
        try {
            lock.lock();
            return (int) fileQueue.getQueueSize();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(byte[] bytes) {
        try {
            lock.lock();
            fileQueue.add(bytes);
            return true;
        } catch (FileFormatException | IOException e) {
            log.error("offer queue error", e);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] poll() {
        try {
            lock.lock();
            return fileQueue.readNextAndRemove();
        } catch (FileFormatException | IOException e) {
            log.error("poll queue error", e);
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        try {
            lock.lock();
            return fileQueue.readNext();
        } catch (FileFormatException | IOException e) {
            log.error("peek queue error", e);
            return null;
        } finally {
            lock.unlock();
        }
    }
}
