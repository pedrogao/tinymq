package github.io.pedrogao.diskqueue.page;

import github.io.pedrogao.diskqueue.TestUtil;
import github.io.pedrogao.diskqueue.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class MappedPageFactoryImplTest {
    private IMappedPageFactory mappedPageFactory;
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "bigqueue", "unit", "mapped_page_factory_test").toString();

    @Test
    public void testGetBackPageFileSet() throws IOException {
        mappedPageFactory = new MappedPageFactoryImpl(1024, Path.of(testDir, "test_get_backpage_fileset").toString(), 2 * 1000);

        for (int i = 0; i < 10; i++) {
            mappedPageFactory.acquirePage(i);
        }

        Set<String> fileSet = mappedPageFactory.getBackPageFileSet();
        assertEquals(10, fileSet.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(fileSet.contains(MappedPageFactoryImpl.PAGE_FILE_NAME + "-" + i + MappedPageFactoryImpl.PAGE_FILE_SUFFIX));
        }
    }

    @Test
    public void testGetBackPageFileSize() throws IOException {
        mappedPageFactory = new MappedPageFactoryImpl(1024 * 1024, Path.of(testDir, "test_get_backpage_filesize").toString(), 2 * 1000);

        for (int i = 0; i < 100; i++) {
            mappedPageFactory.acquirePage(i);
        }

        assertEquals(1024 * 1024 * 100, mappedPageFactory.getBackPageFileSize());
    }


    @Test
    public void testSingleThread() throws IOException {

        mappedPageFactory = new MappedPageFactoryImpl(1024 * 1024 * 128, Path.of(testDir, "test_single_thread").toString(), 2 * 1000);

        IMappedPage mappedPage = mappedPageFactory.acquirePage(0); // first acquire
        assertNotNull(mappedPage);
        IMappedPage mappedPage0 = mappedPageFactory.acquirePage(0); // second acquire
        assertSame(mappedPage, mappedPage0);

        IMappedPage mappedPage1 = mappedPageFactory.acquirePage(1);
        assertNotSame(mappedPage0, mappedPage1);

        mappedPageFactory.releasePage(0); // release first acquire
        mappedPageFactory.releasePage(0); // release second acquire
        TestUtil.sleepQuietly(2200);// let page0 expire
        mappedPageFactory.acquirePage(2);// trigger mark&sweep and purge old page0
        mappedPage = mappedPageFactory.acquirePage(0);// create a new page0
        assertNotSame(mappedPage, mappedPage0);
        TestUtil.sleepQuietly(1000);// let the async cleaner do the job
        assertFalse(mappedPage.isClosed());
        assertTrue(mappedPage0.isClosed());


        for (long i = 0; i < 100; i++) {
            assertNotNull(mappedPageFactory.acquirePage(i));
        }
        assertEquals(100, mappedPageFactory.getCacheSize());
        Set<Long> indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(100, indexSet.size());
        for (long i = 0; i < 100; i++) {
            assertTrue(indexSet.contains(i));
        }

        this.mappedPageFactory.deletePage(0);
        assertEquals(99, mappedPageFactory.getCacheSize());
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(99, indexSet.size());

        this.mappedPageFactory.deletePage(1);
        assertEquals(98, mappedPageFactory.getCacheSize());
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(98, indexSet.size());

        for (long i = 2; i < 50; i++) {
            this.mappedPageFactory.deletePage(i);
        }
        assertEquals(50, mappedPageFactory.getCacheSize());
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(50, indexSet.size());

        this.mappedPageFactory.deleteAllPages();
        assertEquals(0, mappedPageFactory.getCacheSize());
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(0, indexSet.size());

        long start = System.currentTimeMillis();
        for (long i = 0; i < 5; i++) {
            assertNotNull(this.mappedPageFactory.acquirePage(i));
            TestUtil.sleepQuietly(1000);
        }
        indexSet = mappedPageFactory.getPageIndexSetBefore(start - 1000);
        assertEquals(0, indexSet.size());
        indexSet = mappedPageFactory.getPageIndexSetBefore(start + 2500);
        assertEquals(3, indexSet.size());
        indexSet = mappedPageFactory.getPageIndexSetBefore(start + 5000);
        assertEquals(5, indexSet.size());

        mappedPageFactory.deletePagesBefore(start + 2500);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertEquals(2, indexSet.size());
        assertEquals(2, mappedPageFactory.getCacheSize());

        mappedPageFactory.releaseCachedPages();
        assertEquals(0, mappedPageFactory.getCacheSize());
        assertEquals(0, ((MappedPageFactoryImpl) mappedPageFactory).getLockMapSize());
        mappedPageFactory.deleteAllPages();

        start = System.currentTimeMillis();
        for (int i = 0; i <= 100; i++) {
            IMappedPage mappedPageI = mappedPageFactory.acquirePage(i);
            mappedPageI.getLocal(0).put(("hello " + i).getBytes());
            mappedPageI.setDirty(true);
            ((MappedPageImpl) mappedPageI).flush();
            long currentTime = System.currentTimeMillis();
            long iPageFileLastModifiedTime = mappedPageFactory.getPageFileLastModifiedTime(i);
            assertTrue(iPageFileLastModifiedTime >= start);
            assertTrue(iPageFileLastModifiedTime <= currentTime);

            long index = mappedPageFactory.getFirstPageIndexBefore(currentTime + 1);
            assertEquals(index, i);

            start = currentTime;
        }

        mappedPageFactory.deleteAllPages();
    }

    @AfterEach
    public void clear() throws IOException {
        if (this.mappedPageFactory != null) {
            this.mappedPageFactory.deleteAllPages();
        }
        FileUtil.deleteDirectory(new File(testDir));
    }

    @Test
    public void testMultiThreads() throws IOException {
        mappedPageFactory = new MappedPageFactoryImpl(1024 * 1024 * 128, Path.of(testDir, "test_multi_threads").toString(), 2 * 1000);

        int pageNumLimit = 200;
        int threadNum = 1000;

        Map<Integer, IMappedPage[]> sharedMap1 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertEquals(this.mappedPageFactory.getCacheSize(), pageNumLimit);
        Map<Integer, IMappedPage[]> sharedMap2 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertEquals(this.mappedPageFactory.getCacheSize(), pageNumLimit);
        // pages in two maps should be same since they are all cached
        verifyMap(sharedMap1, sharedMap2, threadNum, pageNumLimit, true);
        verifyClosed(sharedMap1, threadNum, pageNumLimit, false);

        TestUtil.sleepQuietly(2500);
        this.mappedPageFactory.acquirePage(pageNumLimit + 1); // trigger mark&sweep
        assertEquals(1, this.mappedPageFactory.getCacheSize());
        Map<Integer, IMappedPage[]> sharedMap3 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertEquals(this.mappedPageFactory.getCacheSize(), pageNumLimit + 1);
        // pages in two maps should be different since all pages in sharedMap1 has expired and purged out
        verifyMap(sharedMap1, sharedMap3, threadNum, pageNumLimit, false);
        verifyClosed(sharedMap3, threadNum, pageNumLimit, false);

        verifyClosed(sharedMap1, threadNum, pageNumLimit, true);

        // ensure no memory leak
        assertEquals(0, ((MappedPageFactoryImpl) mappedPageFactory).getLockMapSize());
    }

    private void verifyClosed(Map<Integer, IMappedPage[]> map, int threadNum, int pageNumLimit, boolean closed) {
        for (int i = 0; i < threadNum; i++) {
            IMappedPage[] pageArray = map.get(i);
            for (int j = 0; j < pageNumLimit; j++) {
                if (closed) {
                    assertTrue(pageArray[j].isClosed());
                } else {
                    assertFalse(pageArray[j].isClosed());
                }
            }
        }
    }

    private void verifyMap(Map<Integer, IMappedPage[]> map1, Map<Integer, IMappedPage[]> map2, int threadNum, int pageNumLimit, boolean same) {
        for (int i = 0; i < threadNum; i++) {
            IMappedPage[] pageArray1 = map1.get(i);
            IMappedPage[] pageArray2 = map2.get(i);
            for (int j = 0; j < pageNumLimit; j++) {
                if (same) {
                    assertSame(pageArray1[j], pageArray2[j]);
                } else {
                    assertNotSame(pageArray1[j], pageArray2[j]);
                }
            }
        }
    }

    private Map<Integer, IMappedPage[]> testAndGetSharedMap(IMappedPageFactory pageFactory, int threadNum, int pageNumLimit) {

        // init shared map
        Map<Integer, IMappedPage[]> sharedMap = new ConcurrentHashMap<>();
        for (int i = 0; i < threadNum; i++) {
            IMappedPage[] pageArray = new IMappedPage[pageNumLimit];
            sharedMap.put(i, pageArray);
        }

        // init threads and start
        CountDownLatch latch = new CountDownLatch(threadNum);
        Worker[] workers = new Worker[threadNum];
        for (int i = 0; i < threadNum; i++) {
            workers[i] = new Worker(i, sharedMap, mappedPageFactory, pageNumLimit, latch);
            workers[i].start();
        }

        // wait to finish
        for (int i = 0; i < threadNum; i++) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                // ignore silently
            }
        }

        // validate
        IMappedPage[] firstPageArray = sharedMap.get(0);
        for (int j = 0; j < pageNumLimit; j++) {
            IMappedPage page = firstPageArray[j];
            assertFalse(page.isClosed());
        }
        for (int i = 1; i < threadNum; i++) {
            IMappedPage[] pageArray = sharedMap.get(i);
            for (int j = 0; j < pageNumLimit; j++) {
                assertSame(firstPageArray[j], pageArray[j]);
            }
        }

        return sharedMap;
    }

    private static class Worker extends Thread {
        private final Map<Integer, IMappedPage[]> map;
        private final IMappedPageFactory pageFactory;
        private final int id;
        private final int pageNumLimit;
        private final CountDownLatch latch;

        public Worker(int id, Map<Integer, IMappedPage[]> sharedMap, IMappedPageFactory mappedPageFactory, int pageNumLimit, CountDownLatch latch) {
            this.map = sharedMap;
            this.pageFactory = mappedPageFactory;
            this.id = id;
            this.pageNumLimit = pageNumLimit;
            this.latch = latch;
        }

        public void run() {
            List<Integer> pageNumList = new ArrayList<>();
            for (int i = 0; i < pageNumLimit; i++) {
                pageNumList.add(i);
            }
            Collections.shuffle(pageNumList);
            IMappedPage[] pages = map.get(id);
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e1) {
                // ignore silently
            }
            for (int i : pageNumList) {
                try {
                    pages[i] = this.pageFactory.acquirePage(i);
                    this.pageFactory.releasePage(i);
                } catch (IOException e) {
                    fail("Got IOException when acquiring page " + i);
                }
            }
        }
    }
}