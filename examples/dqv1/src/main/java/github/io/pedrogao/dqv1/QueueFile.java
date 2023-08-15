package github.io.pedrogao.dqv1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueFile {

    public static final long QUEUE_FILE_LIMIT_LENGTH = 1024 * 1024 * 1024; // 1G

    public static final String MAGIC = "dqv1"; // dqv1

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong readOffset = new AtomicLong();
    private final AtomicLong writeOffset = new AtomicLong();
    private final AtomicLong size = new AtomicLong();
    private boolean closed = false;

    private long fileLimitLength = QUEUE_FILE_LIMIT_LENGTH;

    private final Logger log = LoggerFactory.getLogger(QueueFile.class);

    public QueueFile(String path) throws IOException {
        this(new File(path), false);
    }

    public QueueFile(File file) throws IOException {
        this(file, false);
    }

    public QueueFile(File file, boolean create) throws IOException {
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

    private void mapFile() throws IOException {
        randomAccessFile = new RandomAccessFile(file, "rwd");
        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLimitLength);
    }

    public byte[] peek() {
        if (isClosed())
            return null;

        try {
            lock.readLock().lock();

            if (readOffset.get() == writeOffset.get() && size.get() == 0) {
                return null;
            }

            mappedByteBuffer.position((int) readOffset.get());
            int itemLength = mappedByteBuffer.getInt(); // read item length
            byte[] itemPayload = new byte[itemLength];
            mappedByteBuffer.get(itemPayload);

            return itemPayload;
        } finally {
            lock.readLock().unlock();
        }
    }

    public byte[] read() {
        if (isClosed())
            return null;

        try {
            lock.writeLock().lock();

            if (readOffset.get() == writeOffset.get() && size.get() == 0) {
                return null;
            }

            mappedByteBuffer.position((int) readOffset.get());
            int itemLength = mappedByteBuffer.getInt(); // read item length
            byte[] itemPayload = new byte[itemLength];
            mappedByteBuffer.get(itemPayload);

            readOffset.addAndGet(itemPayload.length + 4); // update read offset
            size.decrementAndGet(); // update size

            flushOffsets();

            return itemPayload;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void write(byte[] bytes) {
        if (isClosed())
            return;
        try {
            lock.writeLock().lock();

            if (writeOffset.get() + bytes.length + 4 > fileLimitLength) {
                log.error("queue file {} write error, item length {} > limit length {}", file.getAbsolutePath(), bytes.length, fileLimitLength);
                throw new IllegalStateException("out of limit length");
            }

            mappedByteBuffer.position((int) writeOffset.get());
            mappedByteBuffer.putInt(bytes.length); // write item length
            mappedByteBuffer.put(bytes); // write item payload

            writeOffset.addAndGet(bytes.length + 4); // update write offset
            size.incrementAndGet(); // update size

            flushOffsets();
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

            flushOffsets();

            mappedByteBuffer.force();
            mappedByteBuffer.clear();

            fileChannel.close();
            randomAccessFile.close();

            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long size() {
        try {
            lock.readLock().lock();

            return size.get();
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

    private void flushOffsets() {
        mappedByteBuffer.position(4);
        mappedByteBuffer.putLong(readOffset.get());
        mappedByteBuffer.putLong(writeOffset.get());
        mappedByteBuffer.putLong(size.get());
        mappedByteBuffer.force();
    }

    private void initIndexData() {
        // 4 + 8 + 8 + 8 = 28
        readOffset.set(28);
        writeOffset.set(28);
        size.set(0);

        mappedByteBuffer.put(MAGIC.getBytes());
        mappedByteBuffer.putLong(readOffset.get());
        mappedByteBuffer.putLong(writeOffset.get());
        mappedByteBuffer.putLong(size.get());
        mappedByteBuffer.force();
    }

    private void loadIndexData() {
        byte[] bytes = new byte[MAGIC.getBytes().length]; // 4
        mappedByteBuffer.get(bytes);

        String magicString = new String(bytes);
        if (!magicString.equals(MAGIC)) {
            log.error("queue file {} format error, magic invalid", file.getAbsolutePath());
            throw new IllegalStateException("invalid queue file");
        }

        readOffset.set(mappedByteBuffer.getLong());
        writeOffset.set(mappedByteBuffer.getLong());
        size.set(mappedByteBuffer.getLong());
    }

    @Override
    public String toString() {
        return "QueueFile{" +
                "readOffset=" + readOffset +
                ", writeOffset=" + writeOffset +
                ", size=" + size +
                ", closed=" + closed +
                '}';
    }
}
