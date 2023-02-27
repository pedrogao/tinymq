package github.io.pedrogao.diskqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FanOutQueueImplTest {
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "foqueue", "unit").toString();
    private IFanOutQueue foQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            foQueue = new FanOutQueueImpl(testDir, "simple_test");
            assertNotNull(foQueue);

            String fid = "simpleTest";
            for (int j = 1; j <= 3; j++) {
                assertEquals(0L, foQueue.size(fid));
                assertTrue(foQueue.isEmpty(fid));

                assertNull(foQueue.dequeue(fid));
                assertNull(foQueue.peek(fid));


                foQueue.enqueue("hello".getBytes());
                assertEquals(1L, foQueue.size(fid));
                assertFalse(foQueue.isEmpty(fid));
                assertEquals("hello", new String(foQueue.peek(fid)));
                assertEquals("hello", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

                foQueue.enqueue("world".getBytes());
                foQueue.flush();
                assertEquals(1L, foQueue.size(fid));
                assertFalse(foQueue.isEmpty(fid));
                assertEquals("world", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

            }

            foQueue.close();
        }
    }

    @Test
    public void clientManagedIndexTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "client_managed_index");
        assertNotNull(foQueue);
        assertTrue(foQueue.isEmpty());

        int loop = 1000;
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue((""+i).getBytes());
            assertEquals(foQueue.size(), i + 1L);
            assertFalse(foQueue.isEmpty());
            assertEquals("" + i, new String(foQueue.get(i)));
        }
    }

    @Test
    public void bigLoopTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "big_loop_test");
        assertNotNull(foQueue);

        int loop = 1000;
        String fid1 = "bigLoopTest1";
        long ts = -1;
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
            assertEquals(foQueue.size(fid1), i + 1L);
            assertFalse(foQueue.isEmpty(fid1));
            byte[] data = foQueue.peek(fid1);
            assertEquals("0", new String(data));
            int length = foQueue.peekLength(fid1);
            assertEquals(1, length);
            if (ts == -1) {
                ts = foQueue.peekTimestamp(fid1);
            } else {
                assertEquals(ts, foQueue.peekTimestamp(fid1));
            }
        }

        assertEquals(foQueue.size(fid1), loop);
        assertFalse(foQueue.isEmpty(fid1));
        assertEquals("0", new String(foQueue.peek(fid1)));

        foQueue.close();

        // create a new instance on exiting queue
        foQueue = new FanOutQueueImpl(testDir, "big_loop_test");
        assertEquals(foQueue.size(fid1), loop);
        assertFalse(foQueue.isEmpty(fid1));

        for(int i = 0; i < loop; i++) {
            byte[] data = foQueue.dequeue(fid1);
            assertEquals("" + i, new String(data));
            assertEquals(foQueue.size(fid1), loop - i - 1);
        }

        assertTrue(foQueue.isEmpty(fid1));

        // fan out test
        String fid2 = "bigLoopTest2";
        assertEquals(foQueue.size(fid2), loop);
        assertFalse(foQueue.isEmpty(fid2));
        assertEquals("0", new String(foQueue.peek(fid2)));

        for(int i = 0; i < loop; i++) {
            byte[] data = foQueue.dequeue(fid2);
            assertEquals("" + i, new String(data));
            assertEquals(foQueue.size(fid2), loop - i - 1);
        }

        assertTrue(foQueue.isEmpty(fid2));

        foQueue.close();
    }

    @Test
    public void loopTimingTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "loop_timing_test");
        assertNotNull(foQueue);

        String fid1 = "loopTimingTest1";
        int loop = 10000;
        long begin = System.currentTimeMillis();
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
        }
        long end = System.currentTimeMillis();
        int timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for(int i = 0; i < loop; i++) {
            assertEquals("" + i, new String(foQueue.dequeue(fid1)));
        }
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Fanout test 1, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");

        String fid2 = "loopTimingTest2";
        begin = System.currentTimeMillis();
        for(int i = 0; i < loop; i++) {
            assertEquals("" + i, new String(foQueue.dequeue(fid2)));
        }
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Fanout test 2, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
    }

    @Test
    public void invalidDataPageSizeTest() throws IOException {
        try {
            foQueue = new FanOutQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE - 1);
            fail("should throw invalid page size exception");
        } catch (IllegalArgumentException ignored) {
            // expected
        }
        // ok
        foQueue = new FanOutQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
    }


    @Test
    public void resetQueueFrontIndexTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "reset_queue_front_index");
        assertNotNull(foQueue);

        String fid = "resetQueueFrontIndex";
        int loop = 1000;
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
        }

        assertEquals("0", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, 1L);
        assertEquals("1", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, 234L);
        assertEquals("234", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, loop - 1);
        assertEquals((loop - 1) + "", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, loop);
        assertNull(foQueue.peek(fid));

        try {
            foQueue.resetQueueFrontIndex(fid, loop + 1);
            fail("should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
    }

    @Test
    public void removeBeforeTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "remove_before", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);

        String randomString1 = TestUtil.randomString(32);
        for(int i = 0; i < 1024 * 10; i++) {
            foQueue.enqueue(randomString1.getBytes());
        }

        String fid = "removeBeforeTest";
        assertEquals(1024 * 10, foQueue.size(fid));

        long timestamp = System.currentTimeMillis();
        String randomString2 = TestUtil.randomString(32);
        for(int i = 0; i < 1024 * 10; i++) {
            foQueue.enqueue(randomString2.getBytes());
        }

        foQueue.removeBefore(timestamp);

        timestamp = System.currentTimeMillis();
        String randomString3 = TestUtil.randomString(32);
        for(int i = 0; i < 1024 * 10; i++) {
            foQueue.enqueue(randomString3.getBytes());
        }

        foQueue.removeBefore(timestamp);

        assertEquals(3 * 10 * 1024, foQueue.size(fid));
    }

    @Test
    public void findClosestIndexTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "find_closest_index", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        assertEquals(IBigArray.NOT_FOUND, foQueue.findClosestIndex(System.currentTimeMillis()));

        int loop = 1000;
        long begin = System.currentTimeMillis();
        TestUtil.sleepQuietly(500);
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
        }
        long midTs1 = System.currentTimeMillis();
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
        }
        long midTs2 = System.currentTimeMillis();
        for(int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
        }

        TestUtil.sleepQuietly(500);
        long end = System.currentTimeMillis();

        assertEquals(0L, foQueue.findClosestIndex(begin));
        assertEquals(3 * loop - 1, foQueue.findClosestIndex(end));

        assertEquals(0L, foQueue.findClosestIndex(IFanOutQueue.EARLIEST));
        assertEquals(3 * loop, foQueue.findClosestIndex(IFanOutQueue.LATEST));

        long midIndex1 = foQueue.findClosestIndex(midTs1);
        System.out.println("mid index = " + midIndex1);
        long midIndex2 = foQueue.findClosestIndex(midTs2);
        System.out.println("mid index = " + midIndex2);
        assertTrue(0L < midIndex1);
        assertTrue(midIndex1 < midIndex2);
        assertTrue(3 * loop -1 > midIndex2);

        long closestTime = foQueue.getTimestamp(midIndex1);
        long closestTimeBefore = foQueue.getTimestamp(midIndex1 - 1);
        long closestTimeAfter = foQueue.getTimestamp(midIndex1 + 1);
        assertTrue(closestTimeBefore <= closestTime);
        assertTrue(closestTimeAfter >= closestTime);
    }

    @Test
    public void findClosestIndexTest2() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "find_closest_index2", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        assertEquals(IBigArray.NOT_FOUND, foQueue.findClosestIndex(System.currentTimeMillis()));

        int loop = 100;
        long[] tsArray = new long[loop];
        for(int i = 0; i < loop; i++) {
            TestUtil.sleepQuietly(10);
            foQueue.enqueue(("" + i).getBytes());
            tsArray[i] = System.currentTimeMillis();
        }

        for(int i = 0; i < loop; i++) {
            long index = foQueue.findClosestIndex(tsArray[i]);
            assertEquals(index, i);
        }
    }

    @Test
    public void limitBackFileSizeTest() throws IOException {
        foQueue = new FanOutQueueImpl(testDir, "limit_back_file_size", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        int oneM = 1024 * 1024;

        String randomString1 = TestUtil.randomString(32);
        for(int i = 0; i < oneM; i++) { // 5 data pages + 5 index page
            foQueue.enqueue(randomString1.getBytes());
        }
        String randomString2 = TestUtil.randomString(32);
        for(int i = 0; i < oneM; i++) { // 5 data pages + 5 index page
            foQueue.enqueue(randomString2.getBytes());
        }
        String randomString3 = TestUtil.randomString(32);
        for(int i = 0; i < oneM; i++) { // 5 data pages + 5 index page
            foQueue.enqueue(randomString3.getBytes());
        }


        assertEquals(3 * oneM, foQueue.size("test"));
        assertEquals(randomString1, new String(foQueue.dequeue("test")));
        assertEquals(randomString1, new String(foQueue.get(0)));
        assertEquals(6 * 32 * oneM, foQueue.getBackFileSize());

        foQueue.limitBackFileSize(oneM * 4 * 32);
        TestUtil.sleepQuietly(500);
        assertEquals(2 * oneM, foQueue.size("test"));
        assertEquals(4 * 32 * oneM, foQueue.getBackFileSize());
        assertEquals(randomString2, new String(foQueue.dequeue("test")));

        foQueue.limitBackFileSize(oneM * 2 * 32);
        TestUtil.sleepQuietly(500);
        assertEquals(1 * oneM, foQueue.size("test"));
        assertEquals(2 * 32 * oneM, foQueue.getBackFileSize());
        assertEquals(randomString3, new String(foQueue.dequeue("test")));

        foQueue.limitBackFileSize(oneM * 32); // will be ignore
        TestUtil.sleepQuietly(500);
        assertEquals(1 * oneM - 1, foQueue.size("test"));
        assertEquals(2 * 32 * oneM, foQueue.getBackFileSize());
        assertEquals(randomString3, new String(foQueue.dequeue("test")));
    }

    @AfterEach
    public void clean() throws IOException {
        if (foQueue != null) {
            foQueue.removeAll();
        }
    }
}