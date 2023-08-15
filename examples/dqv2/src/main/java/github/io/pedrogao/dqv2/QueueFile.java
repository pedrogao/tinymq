package github.io.pedrogao.dqv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueFile {

    public static final long HEAP_FILE_LIMIT_LENGTH = 1024 * 1024 * 1024; // 1G

    private File file;
    private IndexFile indexFile;
    private HeapFile writerHeapFile;
    private HeapFile readerHeapFile;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private boolean closed = false;

    private long fileLimitLength = HEAP_FILE_LIMIT_LENGTH; // cannot change once created

    private final Logger log = LoggerFactory.getLogger(QueueFile.class);

    public QueueFile(String dir) throws IOException {
        this(new File(dir), false);
    }

    public QueueFile(File file) throws IOException {
        this(file, false);
    }

    public QueueFile(File file, long fileLimitLength, boolean create) throws IOException {
        this.fileLimitLength = fileLimitLength;

        if (!file.isDirectory()) {
            log.error("queue file {} is not a directory", file.getAbsolutePath());
            throw new IllegalStateException(file.getAbsolutePath() + " is not a directory");
        }
        this.file = file;
        if (!file.exists() || create) {
            file.createNewFile();
        }
        if (!file.isDirectory()) {
            log.error("queue file {} is not a directory", file.getAbsolutePath());
            throw new IllegalStateException(file.getAbsolutePath() + " is not a directory");
        }

        indexFile = new IndexFile(new File(file, "index"), create);
        int readerIndex = indexFile.getReaderIndex();
        int writerIndex = indexFile.getWriterIndex();
        File readerFile = new File(file, getFormatFileName(readerIndex));
        File writerFile = new File(file, getFormatFileName(writerIndex));
        readerHeapFile = new HeapFile(readerFile, fileLimitLength, create);
        writerHeapFile = new HeapFile(writerFile, fileLimitLength, create);
    }

    public QueueFile(File file, boolean create) throws IOException {
        this(file, HEAP_FILE_LIMIT_LENGTH, create);
    }

    private String getFormatFileName(int index) {
        return String.format("%010d", index);
    }

    public byte[] peek() {
        if (isClosed())
            return null;

        try {
            lock.readLock().lock();

            long readOffset = indexFile.getReadOffset();
            long writeOffset = indexFile.getWriteOffset();
            int readerIndex = indexFile.getReaderIndex();
            int writerIndex = indexFile.getWriterIndex();
            long size = indexFile.getSize();
            long readerHeapFileSize = readerHeapFile.getSize();

            if (readerIndex == writerIndex && readOffset == writeOffset && size == 0) {
                return null;
            }

            // read next file but not switch
            if (readerIndex < writerIndex && readOffset >= readerHeapFileSize) {
                var readerHeapFile = new HeapFile(new File(file, getFormatFileName(readerIndex + 1)), fileLimitLength, true);
                return readerHeapFile.read(HeapFile.META_DATA_LENGTH);
            }

            return readerHeapFile.read((int) readOffset);
        } catch (IOException e) {
            log.error("queue file {} create new read file error", file.getAbsolutePath(), e);
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public byte[] read() {
        if (isClosed())
            return null;

        try {
            lock.writeLock().lock();

            long readOffset = indexFile.getReadOffset();
            long writeOffset = indexFile.getWriteOffset();
            int readerIndex = indexFile.getReaderIndex();
            int writerIndex = indexFile.getWriterIndex();
            long queueSize = indexFile.getSize();
            long readerHeapFileSize = readerHeapFile.getSize(); // FIXME

            if (readerIndex == writerIndex && readOffset == writeOffset && queueSize == 0) {
                return null;
            }

            byte[] payload = readerHeapFile.read((int) readOffset);
            long nextReadOffset = indexFile.updateReadOffset(payload.length + 4);// update read offset
            indexFile.decrementSize(); // update queueSize

            if (readerIndex < writerIndex && nextReadOffset >= readerHeapFileSize) {
                // switch read file to writer file
                readerHeapFile.close();
                readerIndex = indexFile.updateReaderIndex(1);
                readerHeapFile = new HeapFile(new File(file, getFormatFileName(readerIndex)), fileLimitLength, true);
                indexFile.setReadOffset(HeapFile.META_DATA_LENGTH);
            }

            flush();
            return payload;
        } catch (IOException e) {
            log.error("queue file {} create new read file error", file.getAbsolutePath(), e);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void write(byte[] bytes) {
        if (isClosed())
            return;
        try {
            lock.writeLock().lock();

            long writeOffset = indexFile.getWriteOffset();
            if (writeOffset + bytes.length + 4 > fileLimitLength) {
                log.info("queue file {} write out of limit, item length {} > limit length {}", file.getAbsolutePath(), bytes.length, fileLimitLength);
                // flush current write heap file, which will out of limit
                writerHeapFile.close();

                // open next write file
                int writerIndex = indexFile.updateWriterIndex(1);
                indexFile.setWriteOffset(HeapFile.META_DATA_LENGTH);
                writeOffset = HeapFile.META_DATA_LENGTH;
                File writerFile = new File(file, getFormatFileName(writerIndex));
                writerHeapFile = new HeapFile(writerFile, fileLimitLength, true);
            }

            writerHeapFile.write((int) writeOffset, bytes);
            indexFile.updateWriteOffset(bytes.length + 4); // update write offset
            indexFile.incrementSize(); // update size

            flush();
        } catch (IOException e) {
            log.error("queue file {} create new write file error", file.getAbsolutePath(), e);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close() throws IOException {
        if (isClosed())
            return;
        log.debug("queue file {} close", file.getAbsolutePath());

        try {
            lock.writeLock().lock();

            flush();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long size() {
        try {
            lock.readLock().lock();

            return indexFile.getSize();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isClosed() {
        try {
            lock.readLock().lock();

            return closed;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void flush() {
        indexFile.flush();
        readerHeapFile.flush();
        writerHeapFile.flush();
    }
}
