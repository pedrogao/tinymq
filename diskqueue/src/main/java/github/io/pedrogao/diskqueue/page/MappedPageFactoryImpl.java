package github.io.pedrogao.diskqueue.page;

import github.io.pedrogao.diskqueue.cache.ILRUCache;
import github.io.pedrogao.diskqueue.cache.LRUCacheImpl;
import github.io.pedrogao.diskqueue.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class MappedPageFactoryImpl implements IMappedPageFactory {
    private final static Logger logger = LoggerFactory.getLogger(MappedPageFactoryImpl.class);

    private final int pageSize;
    private String pageDir;
    private final File pageDirFile;
    private final String pageFile;
    private final long ttl;

    private final Object mapLock = new Object();
    private final Map<Long, Object> pageCreationLockMap = new HashMap<>();

    public static final String PAGE_FILE_NAME = "page";
    public static final String PAGE_FILE_SUFFIX = ".dat";

    private final ILRUCache<Long, MappedPageImpl> cache;

    public MappedPageFactoryImpl(int pageSize, String pageDir, long cacheTTL) {
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = cacheTTL;
        this.pageDirFile = new File(this.pageDir);
        if (!pageDirFile.exists()) {
            pageDirFile.mkdirs();
        }
        if (!this.pageDir.endsWith(File.separator)) {
            this.pageDir += File.separator;
        }
        this.pageFile = this.pageDir + PAGE_FILE_NAME + "-";
        this.cache = new LRUCacheImpl<>();
    }

    @Override
    public IMappedPage acquirePage(long index) throws IOException {
        Optional<MappedPageImpl> pageOptional = cache.get(index);
        if (pageOptional.isEmpty()) {
            try {
                Object lock = null;
                synchronized (mapLock) {
                    if (!pageCreationLockMap.containsKey(index)) {
                        pageCreationLockMap.put(index, new Object());
                    }
                    lock = pageCreationLockMap.get(index);
                }
                synchronized (lock) {
                    pageOptional = cache.get(index);
                    if (pageOptional.isEmpty()) {
                        RandomAccessFile randomAccessFile = null;
                        FileChannel channel = null;
                        try {
                            String fileName = this.getFileNameByIndex(index);
                            randomAccessFile = new RandomAccessFile(fileName, "rw");
                            channel = randomAccessFile.getChannel();
                            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, this.pageSize);
                            pageOptional = Optional.of(new MappedPageImpl(buffer, fileName, index));
                            cache.put(index, pageOptional.get(), ttl);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Mapped page for " + fileName + " was just created and cached.");
                            }
                        } finally {
                            if (channel != null)
                                channel.close();
                            if (randomAccessFile != null)
                                randomAccessFile.close();
                        }
                    }
                }
            } finally {
                synchronized (mapLock) {
                    pageCreationLockMap.remove(index);
                }
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Hit mapped page " + pageOptional.get().getPageFile() + " in cache.");
            }
        }
        return pageOptional.get();
    }

    private String getFileNameByIndex(long index) {
        return this.pageFile + index + PAGE_FILE_SUFFIX;
    }

    @Override
    public void releasePage(long index) {
        cache.release(index);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String getPageDir() {
        return pageDir;
    }

    @Override
    public void deletePage(long index) throws IOException {
        cache.remove(index);
        String fileName = getFileNameByIndex(index);
        int count = 0;
        int maxRound = 10;
        boolean deleted = false;
        while (count < maxRound) { // retry 10 times
            try {
                FileUtil.deleteFile(new File(fileName));
                deleted = true;
                break;
            } catch (IOException ex) {
                if (logger.isDebugEnabled()) {
                    logger.warn("fail to delete file", ex);
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
                count++;
                if (logger.isDebugEnabled()) {
                    logger.warn("fail to delete file " + fileName + ", tried round = " + count);
                }
            }
        }
        if (deleted) {
            logger.info("Page file " + fileName + " was just deleted.");
        } else {
            logger.warn(
                    "fail to delete file " + fileName + " after max " + maxRound + " rounds of try, you may delete it manually.");
        }
    }

    @Override
    public void deletePages(Set<Long> indexes) throws IOException {
        if (indexes == null || indexes.isEmpty())
            return;
        for (long index : indexes) {
            deletePage(index);
        }
    }

    @Override
    public void deleteAllPages() throws IOException {
        cache.removeAll();
        Set<Long> indexSet = getExistingBackFileIndexSet();
        this.deletePages(indexSet);
        if (logger.isDebugEnabled()) {
            logger.debug("All page files in dir " + this.pageDir + " have been deleted.");
        }
    }

    @Override
    public void releaseCachedPages() throws IOException {
        cache.removeAll();
    }

    @Override
    public Set<Long> getPageIndexSetBefore(long timestamp) {
        Set<Long> beforeIndexSet = new HashSet<>();
        File[] pageFiles = pageDirFile.listFiles();
        if (pageFiles != null) {
            for (File pageFile : pageFiles) {
                if (pageFile.lastModified() < timestamp) {
                    String fileName = pageFile.getName();
                    if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                        long index = this.getIndexByFileName(fileName);
                        beforeIndexSet.add(index);
                    }
                }
            }
        }
        return beforeIndexSet;
    }

    @Override
    public void deletePagesBefore(long timestamp) throws IOException {
        Set<Long> indexSet = getPageIndexSetBefore(timestamp);
        deletePages(indexSet);
        if (logger.isDebugEnabled()) {
            logger.debug("All page files in dir [" + this.pageDir + "], before [" + timestamp + "] have been deleted.");
        }
    }

    @Override
    public void deletePagesBeforePageIndex(long pageIndex) throws IOException {
        Set<Long> indexSet = getExistingBackFileIndexSet();
        for (long index : indexSet) {
            if (index < pageIndex) {
                deletePage(index);
            }
        }
    }

    @Override
    public long getPageFileLastModifiedTime(long index) {
        String pageFileName = getFileNameByIndex(index);
        File pageFile = new File(pageFileName);
        if (!pageFile.exists())
            return -1L;
        return pageFile.lastModified();
    }

    @Override
    public long getFirstPageIndexBefore(long timestamp) {
        Set<Long> beforeIndexSet = getPageIndexSetBefore(timestamp);
        if (beforeIndexSet == null || beforeIndexSet.isEmpty())
            return -1L;
        TreeSet<Long> sortedIndexSet = new TreeSet<>(beforeIndexSet);
        return sortedIndexSet.last();
    }

    @Override
    public Set<Long> getExistingBackFileIndexSet() {
        Set<Long> indexSet = new HashSet<>();
        File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    long index = this.getIndexByFileName(fileName);
                    indexSet.add(index);
                }
            }
        }
        return indexSet;
    }

    private long getIndexByFileName(String fileName) {
        int beginIndex = fileName.lastIndexOf('-');
        beginIndex += 1;
        int endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
        String sIndex = fileName.substring(beginIndex, endIndex);
        return Long.parseLong(sIndex);
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    public void flush() {
        cache.getValues().forEach(MappedPageImpl::flush);
    }

    @Override
    public Set<String> getBackPageFileSet() {
        Set<String> fileSet = new HashSet<>();
        File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    fileSet.add(fileName);
                }
            }
        }
        return fileSet;
    }

    @Override
    public long getBackPageFileSize() {
        long totalSize = 0;
        File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    totalSize += pageFile.length();
                }
            }
        }
        return totalSize;
    }

    // for testing
    int getLockMapSize() {
        return this.pageCreationLockMap.size();
    }
}
