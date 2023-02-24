package github.io.pedrogao.diskqueue.cache;

import github.io.pedrogao.diskqueue.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCacheImpl<K, V extends Closeable> implements ILRUCache<K, V> {
    private final static Logger logger = LoggerFactory.getLogger(LRUCacheImpl.class);

    public static final long DEFAULT_TTL = 10 * 1000; // 10s

    private final Map<K, V> map;
    private final Map<K, TTLValue> ttlMap;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Set<K> keysToRemove = new HashSet<>();

    public LRUCacheImpl() {
        map = new HashMap<>();
        ttlMap = new HashMap<>();
    }

    @Override
    public void put(K key, V value, long ttlInMilliSeconds) {
        Collection<V> valuesToClose;
        try {
            writeLock.lock();
            // mark & sweep
            valuesToClose = markAndSweep();
            if (valuesToClose != null && valuesToClose.contains(value)) {
                valuesToClose.remove(value);
            }
            map.put(key, value);
            var ttl = new TTLValue(TimeUtil.now(), ttlInMilliSeconds);
            ttl.refCount.incrementAndGet();
            ttlMap.put(key, ttl);
        } finally {
            writeLock.unlock();
        }
        if (valuesToClose != null && valuesToClose.size() > 0) {
            if (logger.isDebugEnabled()) {
                int size = valuesToClose.size();
                logger.info("Mark&Sweep found " + size + (size > 1 ? " resources" : " resource") + " to close.");
            }
            // remove unused values async
            executorService.execute(new ValueCloser<>(valuesToClose));
        }
    }

    private Collection<V> markAndSweep() {
        Collection<V> valuesToClose = null;
        keysToRemove.clear();
        Set<K> keys = ttlMap.keySet();
        long currentTS = TimeUtil.now();
        for (K key : keys) {
            TTLValue ttl = ttlMap.get(key);
            if (ttl.refCount.get() <= 0 &&
                    (currentTS - ttl.lastAccessedTimestamp.get()) > ttl.ttl) {
                keysToRemove.add(key); // expired
            }
        }

        if (keysToRemove.size() > 0) {
            valuesToClose = new HashSet<>();
            for (K key : keysToRemove) {
                V v = map.remove(key);
                valuesToClose.add(v);
                ttlMap.remove(key);
            }
        }

        return valuesToClose;
    }

    @Override
    public void put(K key, V value) {
        this.put(key, value, DEFAULT_TTL);
    }

    @Override
    public Optional<V> get(K key) {
        try {
            readLock.lock();
            TTLValue ttl = ttlMap.get(key);
            if (ttl != null) {
                ttl.lastAccessedTimestamp.set(TimeUtil.now());
                ttl.refCount.incrementAndGet(); // +1
            }
            return Optional.ofNullable(map.get(key));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void release(K key) {
        try {
            readLock.lock();
            TTLValue ttl = ttlMap.get(key);
            if (ttl != null) {
                ttl.refCount.decrementAndGet(); // -1
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<V> remove(K key) throws IOException {
        try {
            writeLock.lock();
            ttlMap.remove(key);
            V v = map.remove(key);
            if (v != null) {
                v.close(); // close instantly
            }
            return Optional.ofNullable(v);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeAll() throws IOException {
        try {
            writeLock.lock();

            Collection<V> valuesToClose = new HashSet<>(map.values());
            if (!valuesToClose.isEmpty()) {
                for (V v : valuesToClose) {
                    v.close();
                }
            }
            map.clear();
            ttlMap.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public int size() {
        try {
            readLock.lock();
            return map.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<V> getValues() {
        try {
            readLock.lock();
            return new ArrayList<>(map.values());
        } finally {
            readLock.unlock();
        }
    }

    private static class TTLValue {
        AtomicLong lastAccessedTimestamp;
        AtomicLong refCount = new AtomicLong(0);
        long ttl;

        public TTLValue(long ts, long ttl) {
            this.lastAccessedTimestamp = new AtomicLong(ts);
            this.ttl = ttl;
        }
    }

    private static class ValueCloser<V extends Closeable> implements Runnable {
        Collection<V> valuesToClose;

        public ValueCloser(Collection<V> valuesToClose) {
            this.valuesToClose = valuesToClose;
        }

        @Override
        public void run() {
            var size = valuesToClose.size();
            for (V v : valuesToClose) {
                try {
                    if (v != null) {
                        v.close();
                    }
                } catch (IOException e) {
                    // close silently
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("ResourceCloser closed " + size + (size > 1 ? " resources." : " resource."));
            }
        }
    }

    public static void closeExecutorService() {
        executorService.shutdown();
    }
}
