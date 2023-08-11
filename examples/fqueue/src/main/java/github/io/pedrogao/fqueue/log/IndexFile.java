package github.io.pedrogao.fqueue.log;

import github.io.pedrogao.fqueue.exception.FileFormatException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static github.io.pedrogao.fqueue.log.Const.*;

public class IndexFile {

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件操作位置信息
     */
    private String magicString = null;
    private int version = -1;
    private long readerPosition = -1;
    private long writerPosition = -1;
    private long readerIndex = -1;
    private long writerIndex = -1;
    private final AtomicLong size = new AtomicLong(0);

    private final Logger log = LoggerFactory.getLogger(IndexFile.class);

    public IndexFile(String path) throws IOException, FileFormatException {
        File dbFile = new File(path, INDEX_FILE_NAME);

        if (!dbFile.exists()) {
            dbFile.createNewFile();
            file = new RandomAccessFile(dbFile, "rwd");
            init();
        } else {
            file = new RandomAccessFile(dbFile, "rwd");
            if (file.length() < INDEX_LIMIT_LENGTH) {
                log.error("Index file {} format error", path);
                throw new FileFormatException("file format error");
            }
            byte[] bytes = new byte[INDEX_LIMIT_LENGTH];
            file.read(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            bytes = new byte[MAGIC.getBytes().length]; // 6
            buffer.get(bytes);
            magicString = new String(bytes); // fqueue
            version = buffer.getInt(); // 4
            readerPosition = buffer.getLong(); // 8
            writerPosition = buffer.getLong(); // 8
            readerIndex = buffer.getLong(); // 8
            writerIndex = buffer.getLong(); // 8
            long sz = buffer.getLong(); // 8
            // init if consumer all
            if (readerPosition == writerPosition && readerIndex == writerIndex && sz <= 0) {
                init();
            } else {
                size.set(sz);
            }
            fileChannel = file.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH);
        }

    }

    private void init() throws IOException {
        magicString = MAGIC;
        version = 1;
        readerPosition = INDEX_LIMIT_LENGTH;
        writerPosition = INDEX_LIMIT_LENGTH;
        readerIndex = 1;
        writerIndex = 1;

        file.setLength(INDEX_LIMIT_LENGTH);
        file.seek(0);
        file.write(magicString.getBytes());
        file.writeInt(version);
        file.writeLong(readerPosition);
        file.writeLong(writerPosition);
        file.writeLong(readerIndex);
        file.writeLong(writerIndex);
        file.writeLong(0);

        fileChannel = file.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH);
    }


    public void putWriterPosition(long position) {
        mappedByteBuffer.position(18); // 6 + 4 + 8
        mappedByteBuffer.putLong(position);
        writerPosition = position;
    }

    public void putReaderPosition(long position) {
        mappedByteBuffer.position(10); // 6 + 4
        mappedByteBuffer.putLong(position);
        readerPosition = position;
    }

    public void putWriterIndex(long index) {
        mappedByteBuffer.position(26); // 6 + 4 + 8 + 8
        mappedByteBuffer.putLong(index);
        writerIndex = index;
    }

    public void putReaderIndex(long index) {
        mappedByteBuffer.position(34); // 6 + 4 + 8 + 8 + 8
        mappedByteBuffer.putLong(index);
        readerIndex = index;
    }

    public void incrementSize() {
        long size = this.size.incrementAndGet();
        mappedByteBuffer.position(42); // 6 + 4 + 8 + 8 + 8 + 8
        mappedByteBuffer.putLong(size);
    }

    public void decrementSize() {
        long size = this.size.decrementAndGet();
        mappedByteBuffer.position(42); // 6 + 4 + 8 + 8 + 8 + 8
        mappedByteBuffer.putLong(size);
    }

    public String getMagicString() {
        return magicString;
    }

    public int getVersion() {
        return version;
    }

    public long getReaderPosition() {
        return readerPosition;
    }

    public long getWriterPosition() {
        return writerPosition;
    }

    public long getReaderIndex() {
        return readerIndex;
    }

    public long getWriterIndex() {
        return writerIndex;
    }

    public long getSize() {
        return size.get();
    }

    public void clear() throws IOException {
        mappedByteBuffer.clear();
        mappedByteBuffer.force();

        init();
    }

    public void close() throws IOException {
        mappedByteBuffer.force();
        mappedByteBuffer.clear();
        fileChannel.close();
        file.close();

        mappedByteBuffer = null;
        fileChannel = null;
        file = null;
    }

    public String toString() {
        return "magicString=" + magicString + ", version=" + version + ", readerPosition=" + readerPosition
                + ", writerPosition=" + writerPosition + ", readerIndex=" + readerIndex + ", writerIndex="
                + writerIndex + ", size=" + size;
    }
}
