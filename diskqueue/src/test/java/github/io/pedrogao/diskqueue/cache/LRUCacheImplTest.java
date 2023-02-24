package github.io.pedrogao.diskqueue.cache;

import github.io.pedrogao.diskqueue.TestUtil;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class LRUCacheImplTest {

    @Test
    void singleThreadTest() {
        ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<>();

        var obj = new TestObject();
        cache.put(1, obj, 500);

        Optional<TestObject> obj1 = cache.get(1);
        assertEquals(obj1.get(), obj);
        assertEquals(obj1.get(), obj);
        assertFalse(obj1.get().isClosed());

        TestUtil.sleepQuietly(1000); // 1 expired
        cache.put(2, new TestObject()); // trigger mark & sweep
        obj1 = cache.get(1); // but ref=1, so can't evict
        assertTrue(obj1.isPresent());
        assertEquals(obj1.get(), obj);
        assertFalse(obj1.get().isClosed());

        cache.release(1);
        cache.release(1);
        TestUtil.sleepQuietly(1000); // 1 expired
        cache.put(3, new TestObject());
        obj1 = cache.get(1); // not evict
        assertTrue(obj1.isPresent());
        assertEquals(obj1.get(), obj);
        assertFalse(obj1.get().isClosed());

        cache.release(1);
        cache.release(1);
        TestUtil.sleepQuietly(1000); // 1 expired
        TestObject testObject = new TestObject();
        cache.put(4, testObject);
        obj1 = cache.get(1);
        assertTrue(obj1.isEmpty());

        TestUtil.sleepQuietly(1000); // 1s
        assertTrue(obj.isClosed());

        assertEquals(cache.size(), 3);

        try {
            var testObj2 = cache.remove(2);
            assertTrue(testObj2.isPresent());
            assertTrue(testObj2.get().closed);
        } catch (IOException e1) {
            fail("Got IOException when removing test object 2 from the cache.");
        }

        assertEquals(2, cache.size());

        try {
            var testObj3 = cache.remove(3);
            assertTrue((testObj3.isPresent()));
            assertTrue(testObj3.get().closed);
        } catch (IOException e1) {
            fail("Got IOException when removing test object 3 from the cache.");
        }

        assertEquals(1, cache.size());

        try {
            cache.removeAll();
        } catch (IOException e) {
            fail("Got IOException when closing the cache.");
        }

        TestUtil.sleepQuietly(1000); // let the cleaner do the job
        assertTrue(testObject.isClosed());
        assertEquals(0, cache.size());
    }

    @Test
    void multiThreadsTest() {
        ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<>();
        int threadNum = 100;

        Worker[] workers = new Worker[threadNum];

        // initialization
        for (int i = 0; i < threadNum; i++) {
            workers[i] = new Worker(i, cache);
        }

        // run
        for (int i = 0; i < threadNum; i++) {
            workers[i].start();
        }

        // wait to finish
        for (int i = 0; i < threadNum; i++) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        assertEquals(0, cache.size());

        cache = new LRUCacheImpl<>();
        threadNum = 100;

        RandomWorker[] randomWorkers = new RandomWorker[threadNum];
        TestObject[] testObjs = new TestObject[threadNum];

        // initialization
        for (int i = 0; i < threadNum; i++) {
            testObjs[i] = new TestObject();
            cache.put(i, testObjs[i], 2000);
        }
        for (int i = 0; i < threadNum; i++) {
            randomWorkers[i] = new RandomWorker(threadNum, cache);
        }

        // run
        for (int i = 0; i < threadNum; i++) {
            randomWorkers[i].start();
        }

        // wait to finish
        for (int i = 0; i < threadNum; i++) {
            try {
                randomWorkers[i].join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        // verification
        for (int i = 0; i < threadNum; i++) {
            var testObj = cache.get(i);
            assertTrue(testObj.isPresent());
            cache.release(i);
        }
        for (int i = 0; i < threadNum; i++) {
            assertFalse(testObjs[i].isClosed());
        }

        for (int i = 0; i < threadNum; i++) {
            cache.release(i); // release put
        }

        TestUtil.sleepQuietly(1000); // let the test objects expire but not expired
        cache.put(threadNum + 1, new TestObject()); // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            var testObj = cache.get(i);
            assertTrue(testObj.isPresent()); // hasn't expire yet
            cache.release(i);
        }

        TestUtil.sleepQuietly(2010); // let the test objects expire and be expired
        TestObject tmpObj = new TestObject();
        cache.put(threadNum + 1, tmpObj); // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            var testObj = cache.get(i);
            assertTrue(testObj.isEmpty());
        }

        TestUtil.sleepQuietly(1000); // let the cleaner do the job
        for (int i = 0; i < threadNum; i++) {
            assertTrue(testObjs[i].isClosed());
        }

        assertEquals(1, cache.size());

        assertFalse(tmpObj.isClosed());

        try {
            cache.removeAll();
        } catch (IOException e) {
            fail("Got IOException when closing the cache");
        }
        TestUtil.sleepQuietly(1000);
        assertTrue(tmpObj.isClosed());

        assertEquals(0, cache.size());
    }

    private static class Worker extends Thread {
        private final int id;
        private final ILRUCache<Integer, TestObject> cache;

        public Worker(int id, ILRUCache<Integer, TestObject> cache) {
            this.id = id;
            this.cache = cache;
        }

        public void run() {
            TestObject testObj = new TestObject();
            cache.put(id, testObj, 500);

            var testObj2 = cache.get(id);
            assertEquals(testObj, testObj2.get());
            assertEquals(testObj, testObj2.get());
            assertFalse(testObj2.get().isClosed());

            cache.release(id); // release first put
            cache.release(id); // release first get

            TestUtil.sleepQuietly(1000);
            cache.put(id + 1000, new TestObject(), 500); // trigger mark&sweep

            var testObj3 = cache.get(id);
            assertTrue(testObj3.isEmpty());
            TestUtil.sleepQuietly(1000); // let the cleaner do the job
            assertTrue(testObj.isClosed());


            cache.release(id + 1000);
            TestUtil.sleepQuietly(1000);
            cache.put(id + 2000, new TestObject()); // trigger mark&sweep

            try {
                var testObj_id2000 = cache.remove(id + 2000);
                assertTrue(testObj_id2000.isPresent());
                assertTrue(testObj_id2000.get().isClosed());
            } catch (IOException e) {
                fail("Got IOException when removing test object id 2000 from the cache.");
            }

            var testObj4 = cache.get(id + 1000);
            TestUtil.sleepQuietly(1000); // let the cleaner do the job
            assertTrue(testObj4.isEmpty());
        }
    }

    private static class RandomWorker extends Thread {
        private final int idLimit;
        private final Random random = new Random();
        private final ILRUCache<Integer, TestObject> cache;

        public RandomWorker(int idLimit, ILRUCache<Integer, TestObject> cache) {
            this.idLimit = idLimit;
            this.cache = cache;
        }

        public void run() {
            for (int i = 0; i < 10; i++) {
                int id = random.nextInt(idLimit);

                var testObj = cache.get(id);
                assertTrue(testObj.isPresent());
                cache.put(id + 1000, new TestObject(), 1000);
                cache.put(id + 2000, new TestObject(), 1000);
                cache.put(id + 3000, new TestObject(), 1000);
                cache.release(id + 1000);
                cache.release(id + 3000);
                cache.release(id);
                try {
                    var testObj_id2000 = cache.remove(id + 2000);
                    // maybe already removed by other threads
                    testObj_id2000.ifPresent(testObject -> assertTrue(testObject.isClosed()));
                } catch (IOException e) {
                    fail("Got IOException when removing test object id 2000 from the cache.");
                }
            }
        }
    }

    private static class TestObject implements Closeable {
        private volatile boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}