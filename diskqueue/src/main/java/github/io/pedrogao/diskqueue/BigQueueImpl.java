package github.io.pedrogao.diskqueue;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import github.io.pedrogao.diskqueue.page.IMappedPage;
import github.io.pedrogao.diskqueue.page.IMappedPageFactory;
import github.io.pedrogao.diskqueue.page.MappedPageFactoryImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();
    private SettableFuture<byte[]> dequeueFuture;
    private SettableFuture<byte[]> peekFuture;

    public BigQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    public BigQueueImpl(String queueDir, String queueName, int pageSize) throws IOException {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    @Override
    public boolean isEmpty() {
        return queueFrontIndex.get() == innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) throws IOException {
        innerArray.append(data);

        this.completeFutures();
    }

    @Override
    public byte[] dequeue() throws IOException {
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();

            if (isEmpty()) return null;

            queueFrontIndex = this.queueFrontIndex.get();
            byte[] data = innerArray.get(queueFrontIndex);
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex == Long.MAX_VALUE) { // overflow
                nextQueueFrontIndex = 0L;
            } else {
                nextQueueFrontIndex++;
            }
            this.queueFrontIndex.set(nextQueueFrontIndex);

            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);
            return data;
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        this.initializeDequeueFutureIfNecessary();
        return dequeueFuture;
    }

    @Override
    public void removeAll() throws IOException {
        try {
            queueFrontWriteLock.lock();

            innerArray.removeAll();
            queueFrontIndex.set(0L);
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() throws IOException {
        if (isEmpty()) {
            return null;
        }
        return innerArray.get(queueFrontIndex.get());
    }

    @Override
    public ListenableFuture<byte[]> peekAsync() {
        this.initializePeekFutureIfNecessary();
        return peekFuture;
    }

    @Override
    public void applyForEach(ItemIterator iterator) throws IOException {
        try {
            queueFrontWriteLock.lock();

            if (isEmpty()) {
                return;
            }

            long index = queueFrontIndex.get();
            for (long i = index; i < innerArray.size(); i++) {
                iterator.forEach(innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void gc() throws IOException {
        long beforeIndex = queueFrontIndex.get();
        if (beforeIndex == 0L) {
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }

        try {
            innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ignore) {
            // just ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();

            queueFrontIndexPageFactory.flush();
            innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public long size() {
        long qFront = this.queueFrontIndex.get();
        long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return qRear - qFront;
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }

    @Override
    public void close() throws IOException {
        if (queueFrontIndexPageFactory != null) {
            queueFrontIndexPageFactory.releaseCachedPages();
        }

        synchronized (futureLock) {
            if (peekFuture != null) {
                peekFuture.cancel(false);
            }
            if (dequeueFuture != null) {
                dequeueFuture.cancel(false);
            }
        }
        innerArray.close();
    }

    private void completeFutures() {
        synchronized (futureLock) {
            if (peekFuture != null && !peekFuture.isDone()) {
                try {
                    peekFuture.set(this.peek());
                } catch (IOException e) {
                    peekFuture.setException(e);
                }
            }
            if (dequeueFuture != null && !dequeueFuture.isDone()) {
                try {
                    dequeueFuture.set(this.dequeue());
                } catch (IOException e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    private void initializePeekFutureIfNecessary() {
        synchronized (futureLock) {
            if (peekFuture == null || peekFuture.isDone()) {
                peekFuture = SettableFuture.create();
            }
            if (!this.isEmpty()) {
                try {
                    peekFuture.set(this.peek());
                } catch (IOException e) {
                    peekFuture.setException(e);
                }
            }
        }
    }

    private void initializeDequeueFutureIfNecessary() {
        synchronized (futureLock) {
            if (dequeueFuture == null || dequeueFuture.isDone()) {
                dequeueFuture = SettableFuture.create();
            }
            if (!this.isEmpty()) {
                try {
                    dequeueFuture.set(this.dequeue());
                } catch (IOException e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }
}
