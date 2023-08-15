package github.io.pedrogao.dqv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexFile {
    public static final String MAGIC = "dqv2"; // dqv2

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private final AtomicLong readOffset = new AtomicLong();
    private final AtomicLong writeOffset = new AtomicLong();
    private final AtomicInteger readerIndex = new AtomicInteger();
    private final AtomicInteger writerIndex = new AtomicInteger();
    private final AtomicLong size = new AtomicLong();

    private final Logger log = LoggerFactory.getLogger(IndexFile.class);

    public IndexFile(String path) throws IOException {
        this(new File(path), false);
    }

    public IndexFile(File file) throws IOException {
        this(file, false);
    }

    public IndexFile(File file, boolean create) throws IOException {
        this.file = file;
        if (!file.exists() || create) {
            file.createNewFile();
            mapFile();
            initIndexData();
        } else {
            mapFile();
            loadIndexData();
        }
    }

    public long updateReadOffset(long delta) {
        return readOffset.addAndGet(delta);
    }

    public void setReadOffset(long offset) {
        readOffset.set(offset);
    }

    public long getReadOffset() {
        return readOffset.get();
    }

    public long updateWriteOffset(long delta) {
        return writeOffset.addAndGet(delta);
    }

    public void setWriteOffset(long offset) {
        writeOffset.set(offset);
    }

    public long getWriteOffset() {
        return writeOffset.get();
    }

    public int updateReaderIndex(int delta) {
        return readerIndex.addAndGet(delta);
    }

    public void setReaderIndex(int index) {
        readerIndex.set(index);
    }

    public int getReaderIndex() {
        return readerIndex.get();
    }

    public int updateWriterIndex(int delta) {
        return writerIndex.addAndGet(delta);
    }

    public void setWriterIndex(int index) {
        writerIndex.set(index);
    }

    public int getWriterIndex() {
        return writerIndex.get();
    }

    public long updateSize(long delta) {
        return size.addAndGet(delta);
    }

    public long decrementSize() {
        return size.decrementAndGet();
    }

    public long incrementSize() {
        return size.incrementAndGet();
    }

    public void setSize(long size) {
        this.size.set(size);
    }

    public long getSize() {
        return size.get();
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    public void flushIndexData() {
        mappedByteBuffer.position(4);
        mappedByteBuffer.putLong(readOffset.get());
        mappedByteBuffer.putLong(writeOffset.get());
        mappedByteBuffer.putInt(readerIndex.get());
        mappedByteBuffer.putInt(writerIndex.get());
        mappedByteBuffer.putLong(size.get());
        mappedByteBuffer.force();
    }

    private void mapFile() throws IOException {
        randomAccessFile = new RandomAccessFile(file, "rwd");
        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 36);
    }

    private void initIndexData() {
        // 4 + 8 + 8 + 4 + 4 + 8 = 36
        readOffset.set(HeapFile.META_DATA_LENGTH);
        writeOffset.set(HeapFile.META_DATA_LENGTH);
        readerIndex.set(0);
        writerIndex.set(0);
        size.set(0);

        mappedByteBuffer.position(0);
        mappedByteBuffer.put(MAGIC.getBytes());
        mappedByteBuffer.putLong(readOffset.get());
        mappedByteBuffer.putLong(writeOffset.get());
        mappedByteBuffer.putInt(readerIndex.get());
        mappedByteBuffer.putInt(writerIndex.get());
        mappedByteBuffer.putLong(size.get());
        mappedByteBuffer.force();
    }

    private void loadIndexData() {
        mappedByteBuffer.position(0);
        byte[] bytes = new byte[MAGIC.getBytes().length]; // 4
        mappedByteBuffer.get(bytes);

        String magicString = new String(bytes);
        if (!magicString.equals(MAGIC)) {
            log.error("queue file {} format error, magic invalid", file.getAbsolutePath());
            throw new IllegalStateException("invalid queue file");
        }

        readOffset.set(mappedByteBuffer.getLong()); // 8
        writeOffset.set(mappedByteBuffer.getLong()); // 8
        readerIndex.set(mappedByteBuffer.getInt()); // 4
        writerIndex.set(mappedByteBuffer.getInt()); // 4
        size.set(mappedByteBuffer.getLong()); // 8
    }

    @Override
    public String toString() {
        return "IndexFile{" +
                "readOffset=" + readOffset +
                ", writeOffset=" + writeOffset +
                ", readerIndex=" + readerIndex +
                ", writerIndex=" + writerIndex +
                ", size=" + size +
                '}';
    }
}
