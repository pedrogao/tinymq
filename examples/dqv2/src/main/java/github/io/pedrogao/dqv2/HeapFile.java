package github.io.pedrogao.dqv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class HeapFile {

    public static final String MAGIC = "dqv2"; // dqv2

    public static final int META_DATA_LENGTH = 4 + 8;

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    // private final AtomicLong writePosition = new AtomicLong();

    private final Logger log = LoggerFactory.getLogger(HeapFile.class);

    private long fileLimitLength;

    public HeapFile(File file) throws IOException {
        this(file, 1024 * 1024 * 1024, false);
    }

    public HeapFile(File file, long fileLimitLength) throws IOException {
        this(file, fileLimitLength, false);
    }

    public HeapFile(File file, long fileLimitLength, boolean create) throws IOException {
        this.file = file;
        this.fileLimitLength = fileLimitLength;

        if (!file.exists() || create) {
            file.createNewFile();

            mapFile();
            initHeapData();
        } else {
            mapFile();
            loadHeapData();
        }
    }

    private void mapFile() throws IOException {
        randomAccessFile = new RandomAccessFile(file, "rwd");
        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLimitLength);
    }

    public byte[] read(int position) {
        mappedByteBuffer.position(position);
        int itemLength = mappedByteBuffer.getInt(); // read item length
        byte[] itemPayload = new byte[itemLength];
        mappedByteBuffer.get(itemPayload);

        return itemPayload;
    }

    public void writeAndFlush(int position, byte[] bytes) {
        write(position, bytes);
        flush();
    }

    public void write(int position, byte[] bytes) {
        if (position + bytes.length + 4 > fileLimitLength) {
            log.error("heap file {} write error, item length {} > limit length {}", file.getAbsolutePath(), bytes.length, fileLimitLength);
            throw new IllegalStateException("out of limit length");
        }

        mappedByteBuffer.position(position);
        mappedByteBuffer.putInt(bytes.length); // write item length
        mappedByteBuffer.put(bytes); // write item payload

        // update write position
        mappedByteBuffer.position(4);
        long writePosition = mappedByteBuffer.getLong();
        mappedByteBuffer.position(4);
        mappedByteBuffer.putLong(writePosition + bytes.length + 4);
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    public void close() throws IOException {
        log.debug("heap file {} close", file.getAbsolutePath());

        mappedByteBuffer.force();
        mappedByteBuffer.clear();

        fileChannel.close();
        randomAccessFile.close();
    }

    public long getFileLength() {
        return file.length();
    }

    public long getSize() {
        mappedByteBuffer.position(4);
        return mappedByteBuffer.getLong();
    }

    private void initHeapData() {
        mappedByteBuffer.position(0);
        mappedByteBuffer.put(MAGIC.getBytes()); // write magic
        mappedByteBuffer.putLong(META_DATA_LENGTH); // write position
        mappedByteBuffer.force();
    }

    private void loadHeapData() {
        mappedByteBuffer.position(0);
        byte[] bytes = new byte[MAGIC.getBytes().length]; // 4
        mappedByteBuffer.get(bytes);

        String magicString = new String(bytes);
        if (!magicString.equals(MAGIC)) {
            log.error("heap file {} format error, magic invalid", file.getAbsolutePath());
            throw new IllegalStateException("invalid heap file");
        }
    }
}
