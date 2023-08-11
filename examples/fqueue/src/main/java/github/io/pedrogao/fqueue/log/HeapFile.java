package github.io.pedrogao.fqueue.log;

import github.io.pedrogao.fqueue.exception.FileEOFException;
import github.io.pedrogao.fqueue.exception.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static github.io.pedrogao.fqueue.log.Const.*;

public class HeapFile {
    private final Logger log = LoggerFactory.getLogger(HeapFile.class);

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private IndexFile indexFile;
    private long fileLengthLimit;

    private String magicString;
    private int version = -1;
    private long readerPosition = -1;
    private long writerPosition = -1;
    private long endPosition = -1;
    private long currentFileNumber = -1;
    private long nextFileNumber = -1;

    public HeapFile(String path, long fileNumber, long fileLengthLimit,
                    IndexFile indexFile) throws IOException, FileFormatException {
        this(path, fileNumber, fileLengthLimit, indexFile, false);
    }

    public HeapFile(String path, long fileNumber, long fileLengthLimit,
                    IndexFile indexFile, boolean create) throws IOException, FileFormatException {
        this.currentFileNumber = fileNumber;
        this.fileLengthLimit = fileLengthLimit;
        this.indexFile = indexFile;
        this.file = getFormatFile(path, fileNumber);

        if (!file.exists() || create) {
            createHeapLog();
        } else {
            randomAccessFile = new RandomAccessFile(file, "rwd");
            long length = randomAccessFile.length();
            if (length < HEAP_HEAD_LENGTH) {
                log.error("Heap file {} format error", file.getAbsolutePath());
                throw new FileFormatException("file format error");
            }
            if (fileLengthLimit < length) {
                fileLengthLimit = length;
                this.fileLengthLimit = length;
            }
            fileChannel = randomAccessFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLengthLimit);
            byte[] bytes = new byte[MAGIC.getBytes().length]; // 6
            mappedByteBuffer.get(bytes);
            magicString = new String(bytes);
            if (!magicString.equals(MAGIC)) {
                log.error("Heap file {} format error, magic invalid", file.getAbsolutePath());
                throw new FileFormatException("file format error");
            }
            version = mappedByteBuffer.getInt(); // 4
            nextFileNumber = mappedByteBuffer.getLong(); // 8
            endPosition = mappedByteBuffer.getLong(); // 8
            if (endPosition == -1) {
                writerPosition = indexFile.getWriterPosition();
            } else {
                writerPosition = endPosition;
            }
            if (indexFile.getReaderIndex() == currentFileNumber) {
                readerPosition = indexFile.getReaderPosition();
            } else {
                readerPosition = HEAP_HEAD_LENGTH;
            }
        }
        // sync file
        Thread syncThread = new Thread(new Sync());
        syncThread.setDaemon(true);
        syncThread.start();
    }

    public byte write(byte[] bytes) {
        int increment = bytes.length + Integer.BYTES;
        if (isFull(increment)) {
            mappedByteBuffer.position(18);
            mappedByteBuffer.putLong(writerPosition);
            endPosition = writerPosition;
            return WRITE_FULL;
        }
        mappedByteBuffer.position((int) writerPosition);
        mappedByteBuffer.putInt(bytes.length);
        mappedByteBuffer.put(bytes);
        writerPosition += increment;

        indexFile.putWriterPosition(writerPosition);
        return WRITE_SUCCESS;
    }

    public byte[] read(boolean commit) throws FileEOFException {
        if (endPosition != -1 && readerPosition >= endPosition) {
            log.error("Heap file {} reach end of file", file.getAbsolutePath());
            throw new FileEOFException("reach end of file");
        }
        if (readerPosition >= writerPosition) {
            return null;
        }

        mappedByteBuffer.position((int) readerPosition);
        int length = mappedByteBuffer.getInt();
        byte[] bytes = new byte[length];
        mappedByteBuffer.get(bytes);
        if (commit) {
            readerPosition += length + Integer.BYTES;
            indexFile.putReaderPosition(readerPosition);
        }
        return bytes;
    }

    public byte[] read() throws FileEOFException {
        return read(true);
    }

    public void reset() {
        version = -1;
        endPosition = -1;
        currentFileNumber = -1;
        nextFileNumber = -1;

        mappedByteBuffer.position(0);
        mappedByteBuffer.put(MAGIC.getBytes());
        mappedByteBuffer.putInt(version);
        mappedByteBuffer.putLong(nextFileNumber);
        mappedByteBuffer.putLong(endPosition);
        mappedByteBuffer.force();

        magicString = MAGIC;
        writerPosition = HEAP_HEAD_LENGTH;
        readerPosition = HEAP_HEAD_LENGTH;
    }

    public void putNextFileNumber(long number) {
        mappedByteBuffer.position(10); // 6 + 4
        mappedByteBuffer.putLong(number);
        nextFileNumber = number;
    }

    public boolean isFull(int increment) {
        return writerPosition + increment > fileLengthLimit;
    }

    public void close() throws IOException {
        if (mappedByteBuffer == null)
            return;

        mappedByteBuffer.force();
        mappedByteBuffer = null;
        fileChannel.close();
        randomAccessFile.close();
    }

    public long getCurrentFileNumber() {
        return currentFileNumber;
    }

    public long getNextFileNumber() {
        return nextFileNumber;
    }

    @Override
    public String toString() {
        return "HeapFile{" +
                "magicString='" + magicString + '\'' +
                ", version=" + version +
                ", readerPosition=" + readerPosition +
                ", writerPosition=" + writerPosition +
                ", endPosition=" + endPosition +
                ", currentFileNumber=" + currentFileNumber +
                ", nextFileNumber=" + nextFileNumber +
                ", fileLengthLimit=" + fileLengthLimit +
                '}';
    }

    private void createHeapLog() throws IOException {
        randomAccessFile = new RandomAccessFile(file, "rwd");
        randomAccessFile.setLength(0);

        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLengthLimit);
        mappedByteBuffer.put(MAGIC.getBytes());
        mappedByteBuffer.putInt(version);
        mappedByteBuffer.putLong(nextFileNumber);
        mappedByteBuffer.putLong(endPosition);

        magicString = MAGIC;
        writerPosition = HEAP_HEAD_LENGTH;
        readerPosition = HEAP_HEAD_LENGTH;
        indexFile.putWriterPosition(writerPosition);
    }

    private File getFormatFile(String path, long fileNumber) {
        return new File(path, DB_FILE_PREFIX + fileNumber + DB_FILE_SUFFIX);
    }

    public File getFile() {
        return file;
    }

    private class Sync implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (mappedByteBuffer != null) {
                    try {
                        mappedByteBuffer.force();
                    } catch (Exception e) {
                        log.error("Sync heap file {} error", file.getAbsolutePath(), e);
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("Sync heap file {} error", file.getAbsolutePath(), e);
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }
}
