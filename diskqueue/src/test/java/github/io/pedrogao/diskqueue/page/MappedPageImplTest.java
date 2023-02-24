package github.io.pedrogao.diskqueue.page;

import github.io.pedrogao.diskqueue.TestUtil;
import github.io.pedrogao.diskqueue.util.FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class MappedPageImplTest {
    private IMappedPageFactory mappedPageFactory;
    private final String testDir = Path.of(TestUtil.TEST_BASE_DIR, "bigqueue", "unit", "mapped_page_test").toString();

    @Test
    public void testSingleThread() throws IOException {
        int pageSize = 1024 * 1024 * 32;
        mappedPageFactory = new MappedPageFactoryImpl(pageSize, Path.of(testDir, "test_single_thread").toString(), 2 * 1000);

        IMappedPage mappedPage = this.mappedPageFactory.acquirePage(0);
        assertNotNull(mappedPage);

        ByteBuffer buffer = mappedPage.getLocal(0);
        assertEquals(buffer.limit(), pageSize);
        assertEquals(0, buffer.position());

        for (int i = 0; i < 10000; i++) {
            String hello = "hello world";
            int length = hello.getBytes().length;
            mappedPage.getLocal(i * 20).put(hello.getBytes());
            assertArrayEquals(mappedPage.getLocal(i * 20, length), hello.getBytes());
        }

        buffer = ByteBuffer.allocateDirect(16);
        buffer.putInt(1);
        buffer.putInt(2);
        buffer.putLong(3L);
        for (int i = 0; i < 10000; i++) {
            buffer.flip();
            mappedPage.getLocal(i * 20).put(buffer);
        }
        for (int i = 0; i < 10000; i++) {
            ByteBuffer buf = mappedPage.getLocal(i * 20);
            assertEquals(1, buf.getInt());
            assertEquals(2, buf.getInt());
            assertEquals(3L, buf.getLong());
        }
    }

    @Test
    public void testMultiThreads() {
        int pageSize = 1024 * 1024 * 32;
        mappedPageFactory = new MappedPageFactoryImpl(pageSize, Path.of(testDir, "test_multi_threads").toString(), 2 * 1000);

        int threadNum = 100;
        int pageNumLimit = 50;

        Set<IMappedPage> pageSet = Collections.newSetFromMap(new ConcurrentHashMap<IMappedPage, Boolean>());
        List<ByteBuffer> localBufferList = Collections.synchronizedList(new ArrayList<ByteBuffer>());

        Worker[] workers = new Worker[threadNum];
        for (int i = 0; i < threadNum; i++) {
            workers[i] = new Worker(i, mappedPageFactory, pageNumLimit, pageSet, localBufferList);
        }
        for (int i = 0; i < threadNum; i++) {
            workers[i].start();
        }

        for (int i = 0; i < threadNum; i++) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        assertEquals(localBufferList.size(), threadNum * pageNumLimit);
        assertEquals(pageSet.size(), pageNumLimit);

        // verify thread locality
        for (int i = 0; i < localBufferList.size(); i++) {
            for (int j = i + 1; j < localBufferList.size(); j++) {
                if (localBufferList.get(i) == localBufferList.get(j)) {
                    fail("thread local buffer is not thread local");
                }
            }
        }
    }

    private static class Worker extends Thread {
        private final int id;
        private final int pageNumLimit;
        private final IMappedPageFactory pageFactory;
        private final Set<IMappedPage> sharedPageSet;
        private final List<ByteBuffer> localBufferList;

        public Worker(int id, IMappedPageFactory pageFactory, int pageNumLimit,
                      Set<IMappedPage> sharedPageSet, List<ByteBuffer> localBufferList) {
            this.id = id;
            this.pageFactory = pageFactory;
            this.sharedPageSet = sharedPageSet;
            this.localBufferList = localBufferList;
            this.pageNumLimit = pageNumLimit;

        }

        public void run() {
            for (int i = 0; i < pageNumLimit; i++) {
                try {
                    IMappedPage page = this.pageFactory.acquirePage(i);
                    sharedPageSet.add(page);
                    localBufferList.add(page.getLocal(0));

                    int startPosition = this.id * 2048;

                    for (int j = 0; j < 100; j++) {
                        String helloj = "hello world " + j;
                        int length = helloj.getBytes().length;
                        page.getLocal(startPosition + j * 20).put(helloj.getBytes());
                        assertArrayEquals(page.getLocal(startPosition + j * 20, length), helloj.getBytes());
                    }

                    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
                    buffer.putInt(1);
                    buffer.putInt(2);
                    buffer.putLong(3L);
                    for (int j = 0; j < 100; j++) {
                        buffer.flip();
                        page.getLocal(startPosition + j * 20).put(buffer);
                    }
                    for (int j = 0; j < 100; j++) {
                        ByteBuffer buf = page.getLocal(startPosition + j * 20);
                        assertEquals(1, buf.getInt());
                        assertEquals(2, buf.getInt());
                        assertEquals(3L, buf.getLong());
                    }

                } catch (IOException e) {
                    fail("Got IOException when acquiring page " + i);
                }
            }
        }

    }

    @AfterEach
    public void clear() throws IOException {
        if (this.mappedPageFactory != null) {
            this.mappedPageFactory.deleteAllPages();
        }
        FileUtil.deleteDirectory(new File(testDir));
    }
}