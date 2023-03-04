package github.io.pedrogao.diskqueue;

import github.io.pedrogao.diskqueue.page.IMappedPage;
import github.io.pedrogao.diskqueue.page.IMappedPageFactory;
import github.io.pedrogao.diskqueue.page.MappedPageFactoryImpl;
import github.io.pedrogao.diskqueue.util.FolderNameValidator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FanOutQueueImpl implements IFanOutQueue {
    final BigArrayImpl innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name prefix for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX = "front_index_";

    final ConcurrentMap<String, QueueFront> queueFrontMap = new ConcurrentHashMap<>();


    public FanOutQueueImpl(String queueDir, String queueName, int pageSize)
            throws IOException {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);
    }

    public FanOutQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    QueueFront getQueueFront(String fanoutId) throws IOException {
        QueueFront front = queueFrontMap.get(fanoutId);
        if (front == null) {
            front = new QueueFront(fanoutId);
            QueueFront found = queueFrontMap.putIfAbsent(fanoutId, front);
            if (found != null) {
                front.indexPageFactory.releaseCachedPages();
                front = found;
            }
        }

        return front;
    }

    @Override
    public boolean isEmpty(String fanoutId) throws IOException {
        try {
            innerArray.arrayReadLock.lock();
            QueueFront queueFront = getQueueFront(fanoutId);
            return queueFront.index.get() == innerArray.getHeadIndex();
        } finally {
            innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return innerArray.isEmpty();
    }

    @Override
    public long enqueue(byte[] data) throws IOException {
        return innerArray.append(data);
    }

    @Override
    public byte[] dequeue(String fanoutId) throws IOException {
        try {
            innerArray.arrayReadLock.lock();

            QueueFront queueFront = getQueueFront(fanoutId);
            try {
                queueFront.writeLock.lock();

                if (queueFront.index.get() == innerArray.arrayHeadIndex.get()) {
                    return null;
                }

                byte[] data = innerArray.get(queueFront.index.get());
                queueFront.incrementIndex();

                return data;
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
                queueFront.resetIndex();

                byte[] data = innerArray.get(queueFront.index.get());
                queueFront.incrementIndex();
                return data;
            } finally {
                queueFront.writeLock.unlock();
            }
        } finally {
            innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public byte[] peek(String fanoutId) throws IOException {
        try {
            this.innerArray.arrayReadLock.lock();
            QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex()) {
                return null; // empty
            }

            return innerArray.get(qf.index.get());
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public int peekLength(String fanoutId) throws IOException {
        try {
            this.innerArray.arrayReadLock.lock();
            QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex()) {
                return -1; // empty
            }
            return innerArray.getItemLength(qf.index.get());
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public long peekTimestamp(String fanoutId) throws IOException {
        try {
            this.innerArray.arrayReadLock.lock();

            QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex()) {
                return -1; // empty
            }
            return innerArray.getTimestamp(qf.index.get());
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public byte[] get(long index) throws IOException {
        return innerArray.get(index);
    }

    @Override
    public int getLength(long index) throws IOException {
        return innerArray.getItemLength(index);
    }

    @Override
    public long getTimestamp(long index) throws IOException {
        return innerArray.getTimestamp(index);
    }

    @Override
    public long size(String fanoutId) throws IOException {
        try {
            innerArray.arrayReadLock.lock();

            QueueFront qf = this.getQueueFront(fanoutId);
            long qFront = qf.index.get();
            long qRear = innerArray.getHeadIndex();
            if (qFront <= qRear) {
                return (qRear - qFront);
            } else {
                return Long.MAX_VALUE - qFront + 1 + qRear;
            }
        } finally {
            innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public long size() {
        return this.innerArray.size();
    }

    @Override
    public void flush() {
        try {
            this.innerArray.arrayReadLock.lock();

            for (QueueFront qf : this.queueFrontMap.values()) {
                try {
                    qf.writeLock.lock();
                    qf.indexPageFactory.flush();
                } finally {
                    qf.writeLock.unlock();
                }
            }
            innerArray.flush();
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public void removeBefore(long timestamp) throws IOException {
        try {
            this.innerArray.arrayWriteLock.lock();

            this.innerArray.removeBefore(timestamp);
            for (QueueFront qf : this.queueFrontMap.values()) {
                try {
                    qf.writeLock.lock();
                    qf.validateAndAdjustIndex();
                } finally {
                    qf.writeLock.unlock();
                }
            }
        } finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    @Override
    public void limitBackFileSize(long sizeLimit) throws IOException {
        try {
            this.innerArray.arrayWriteLock.lock();

            this.innerArray.limitBackFileSize(sizeLimit);

            for (QueueFront qf : this.queueFrontMap.values()) {
                try {
                    qf.writeLock.lock();
                    qf.validateAndAdjustIndex();
                } finally {
                    qf.writeLock.unlock();
                }
            }

        } finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    @Override
    public long getBackFileSize() throws IOException {
        return this.innerArray.getBackFileSize();
    }

    @Override
    public long findClosestIndex(long timestamp) throws IOException {
        try {
            this.innerArray.arrayReadLock.lock();

            if (timestamp == LATEST) {
                return this.innerArray.getHeadIndex();
            }
            if (timestamp == EARLIEST) {
                return this.innerArray.getTailIndex();
            }

            return this.innerArray.findClosestIndex(timestamp);
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public void resetQueueFrontIndex(String fanoutId, long index) throws IOException {
        try {
            innerArray.arrayReadLock.lock();
            QueueFront qf = this.getQueueFront(fanoutId);

            try {
                qf.writeLock.lock();

                if (index != innerArray.getHeadIndex()) { // ok to set index to array head index
                    innerArray.validateIndex(index);
                }
                qf.index.set(index);
                qf.persistIndex();
            } finally {
                qf.writeLock.unlock();
            }

        } finally {
            innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public void removeAll() throws IOException {
        try {
            this.innerArray.arrayWriteLock.lock();

            for (QueueFront qf : this.queueFrontMap.values()) {
                try {
                    qf.writeLock.lock();
                    qf.index.set(0L);
                    qf.persistIndex();
                } finally {
                    qf.writeLock.unlock();
                }
            }
            innerArray.removeAll();
        } finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    @Override
    public long getFrontIndex() {
        return this.innerArray.getTailIndex();
    }

    @Override
    public long getFrontIndex(String fanoutId) throws IOException {
        try {
            this.innerArray.arrayReadLock.lock();

            QueueFront qf = this.getQueueFront(fanoutId);
            return qf.index.get();
        } finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public long getRearIndex() {
        return this.innerArray.getHeadIndex();
    }

    @Override
    public void close() throws IOException {
        try {
            innerArray.arrayWriteLock.lock();

            for (var qf : queueFrontMap.values()) {
                qf.indexPageFactory.releaseCachedPages();
            }
            innerArray.close();
        } finally {
            innerArray.arrayWriteLock.unlock();
        }
    }

    // Queue front wrapper
    class QueueFront {

        // fanout index
        final String fanoutId;

        // front index of the fanout queue
        final AtomicLong index = new AtomicLong();

        // factory for queue front index page management(acquire, release, cache)
        final IMappedPageFactory indexPageFactory;

        // lock for queue front write management
        final Lock writeLock = new ReentrantLock();

        QueueFront(String fanoutId) throws IOException {
            try {
                FolderNameValidator.validate(fanoutId);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid fanout identifier", ex);
            }
            this.fanoutId = fanoutId;
            // the ttl does not matter here since queue front index page is always cached
            this.indexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                    innerArray.getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX + fanoutId,
                    10 * 1000/*does not matter*/);

            IMappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

            ByteBuffer indexBuffer = indexPage.getLocal(0);
            index.set(indexBuffer.getLong());
            validateAndAdjustIndex();
        }

        void validateAndAdjustIndex() throws IOException {
            if (index.get() != innerArray.arrayHeadIndex.get()) { // ok that index is equal to array head index
                try {
                    innerArray.validateIndex(index.get());
                } catch (IndexOutOfBoundsException ex) { // maybe the back array has been truncated to limit size
                    resetIndex();
                }
            }
        }

        // reset queue front index to the tail of array
        void resetIndex() throws IOException {
            index.set(innerArray.arrayTailIndex.get());

            this.persistIndex();
        }

        void incrementIndex() throws IOException {
            long nextIndex = index.get();
            if (nextIndex == Long.MAX_VALUE) {
                nextIndex = 0L; // wrap
            } else {
                nextIndex++;
            }
            index.set(nextIndex);

            this.persistIndex();
        }

        void persistIndex() throws IOException {
            // persist index
            IMappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer indexBuffer = indexPage.getLocal(0);
            indexBuffer.putLong(index.get());
            indexPage.setDirty(true);
        }
    }
}
