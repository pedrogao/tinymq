package github.io.pedrogao.dqv3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueFile {
    // Index file path, once created cannot change
    private final String path;
    private final String name;

    public static final long DEFAULT_MAX_BYTES_PER_FILE = 1024 * 1024 * 1024; // 1GB
    public static final int DEFAULT_SYNC_INTERVAL = 1000; // 1s

    private long maxBytesPerFile = DEFAULT_MAX_BYTES_PER_FILE; // 1GB
    private long maxBytesPerFileRead = maxBytesPerFile; // 1GB
    private int syncInterval = DEFAULT_SYNC_INTERVAL; // 1s

    // Metadata, both in disk & memory
    private long readPosition = 0;
    private long writePosition = 0;
    private int readFileNum = 0;
    private int writeFileNum = 0;
    private long size = 0;

    // Helper fields
    private long nextReadPosition = 0;
    private int nextReadFileNum = 0;

    private byte[] dataRead; // cache for peek

    // Read & write file
    private RandomAccessFile readFile;
    private RandomAccessFile writeFile;

    // Common fields
    private final Random random = new Random(System.currentTimeMillis());

    private final Logger log = LoggerFactory.getLogger(QueueFile.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public QueueFile(String path) throws IOException {
        this(path, "anonymous", DEFAULT_MAX_BYTES_PER_FILE, DEFAULT_SYNC_INTERVAL);
    }

    public QueueFile(String path, String name) throws IOException {
        this(path, name, DEFAULT_MAX_BYTES_PER_FILE, DEFAULT_SYNC_INTERVAL);
    }

    public QueueFile(String path, String name, long maxBytesPerFile) throws IOException {
        this(path, name, maxBytesPerFile, DEFAULT_SYNC_INTERVAL);
    }

    public QueueFile(String path, String name, long maxBytesPerFile, int syncInterval) throws IOException {
        if (maxBytesPerFile <= 0) {
            throw new IllegalArgumentException("maxBytesPerFile must be positive");
        }
        if (syncInterval <= 0) {
            throw new IllegalArgumentException("syncInterval must be positive");
        }
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be null or empty");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }

        File dir = new File(path);
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("path must be a directory");
        }
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalArgumentException("mkdirs of path failed");
        }

        this.path = path;
        this.name = name;
        this.maxBytesPerFile = maxBytesPerFile;
        this.syncInterval = syncInterval;

        readMetadata();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(syncInterval);

                    readWriteLock.writeLock().lock();
                    sync();
                } catch (Exception e) {
                    log.error("sync queue " + name + " error", e);
                } finally {
                    readWriteLock.writeLock().unlock();
                }
            }
        }).start();
    }


    private void writeMetadata() throws IOException {
        // 1. create a new temp file
        // 2. write metadata to temp file
        // 3. rename temp file to index file
        String path = getMetaFilePath();
        File tempFile = File.createTempFile(path + random.nextInt(), ".tmp");
        try (RandomAccessFile tempRandomAccessFile = new RandomAccessFile(tempFile, "rw")) {
            tempRandomAccessFile.writeLong(readPosition);
            tempRandomAccessFile.writeLong(writePosition);
            tempRandomAccessFile.writeInt(readFileNum);
            tempRandomAccessFile.writeInt(writeFileNum);
            tempRandomAccessFile.writeLong(size);

            // flush to disk
            tempRandomAccessFile.getFD().sync();

            // atomic rename temp file to meta file
            //
            // https://stackoverflow.com/questions/1325388/how-to-find-out-why-renameto-failed
            // It's possible that the reason that renaming failed is that the file is still open.
            //  Even if you are closing the file, it could be held open because of (for example):
            // A file handle is inherited by a subprocess of your process
            // An antivirus program is scanning the file for viruses, and so has it open
            // An indexer (such as Google Desktop or the Windows indexing service) has the file open
            boolean ok = tempFile.renameTo(new File(path));
            if (!ok) {
                throw new IOException("Rename temp file " + tempFile + " to " + path + " index file failed");
            }
        } catch (IOException e) {
            log.error("Flush index file error", e);
            throw new IOException(e);
        }
    }

    private void readMetadata() throws IOException {
        String path = getMetaFilePath();
        File fileInner = new File(path);
        if (!fileInner.exists()) {
            log.info("Index file {} not exists", path);
            return;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileInner, "rw")) {
            randomAccessFile.seek(0);
            readPosition = randomAccessFile.readLong();
            writePosition = randomAccessFile.readLong();
            readFileNum = randomAccessFile.readInt();
            writeFileNum = randomAccessFile.readInt();
            size = randomAccessFile.readLong();
        } catch (IOException e) {
            log.error("Seek index file error", e);
            throw new IOException(e);
        }

        nextReadFileNum = readFileNum;
        nextReadPosition = readPosition;
    }

    private String getDataFilePath(int fileNum) {
        return Path.of(path, String.format("%s.%06d.data", name, fileNum)).toString();
    }

    private String getMetaFilePath() {
        return Path.of(path, name + ".meta").toString();
    }

    public void write(byte[] bytes) throws IOException {
        // TODO
        // 1. to many rotate files
        // 2. rename error
        try {
            readWriteLock.writeLock().lock();
            writeOne(bytes);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public byte[] read() throws IOException {
        // 1. update readPosition & readFileNum & size
        // 2. check readFileNum & nextReadFileNum, if not equal, remove older readFile
        try {
            readWriteLock.writeLock().lock();

            byte[] payload = readCache();
            int oldReadFileNum = readFileNum;
            readPosition = nextReadPosition;
            readFileNum = nextReadFileNum;
            size--;

            if (oldReadFileNum != nextReadFileNum) {
                String oldDataFilePath = getDataFilePath(oldReadFileNum);
                boolean deleted = new File(oldDataFilePath).delete();
                if (!deleted) {
                    log.error("Delete old data file {} failed", oldDataFilePath);
                }
            }
            return payload;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public byte[] peek() throws IOException {
        try {
            readWriteLock.writeLock().lock();

            return readCache();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private byte[] readCache() throws IOException {
        if (readFileNum < writeFileNum || readPosition < writePosition) {
            if (nextReadPosition == readPosition) { // read if necessary
                dataRead = readOne();
            }

            return dataRead;
        }

        // queue is empty, read nothing
        return null;
    }


    private void writeOne(byte[] payload) throws IOException {
        // 1. check if you need to rotate file, current write file reach the end
        //    if so, update writeFileNum, writePosition, maxBytesPerFileRead, sync & close current write file
        // 2. open write file if not opened
        //    if not opened, open write file, and seek writePosition,
        // 3. write one record, including size & payload
        // 4. update writePosition and size
        int size = payload.length;
        int total = 4 + size;
        if (writePosition + total >= maxBytesPerFile) {
            if (readFileNum == writeFileNum) {
                maxBytesPerFileRead = writePosition; // set to `writePosition` because will rotate write file
            }
            writeFileNum++;
            writePosition = 0;
            sync();

            if (writeFile != null) {
                writeFile.close();
                writeFile = null;
            }
        }

        if (writeFile == null) {
            File writeFileInner = new File(getDataFilePath(writeFileNum));
            if (!writeFileInner.exists() && !writeFileInner.createNewFile()) {
                throw new IOException("Create new write file " + writeFileInner + " failed");
            }
            writeFile = new RandomAccessFile(writeFileInner, "rw");
            writeFile.seek(writePosition);
        }

        writeFile.writeInt(size);
        writeFile.write(payload);

        writePosition += total;
        this.size++;
    }

    private byte[] readOne() throws IOException {
        // 1. open read file if not opened
        // 2. read one record, including size & payload
        // 3. update nextReadFileNum & nextReadPosition
        // 4. check if you need to rotate file, current read file reach the end
        //    if so, close current read file, update nextReadFileNum & nextReadPosition
        // 5. return payload
        if (readFile == null) {
            String path = getDataFilePath(readFileNum);
            readFile = new RandomAccessFile(path, "rw");

            if (readPosition != 0) {
                readFile.seek(readPosition);
            }

            maxBytesPerFileRead = maxBytesPerFile;
            // read file is behind write file
            if (readFileNum < writeFileNum) {
                maxBytesPerFileRead = readFile.length();
            }
        }

        int size = readFile.readInt();
        byte[] payload = new byte[size];
        readFile.read(payload);

        int totalSize = 4 + size;
        nextReadPosition = readPosition + totalSize;
        nextReadFileNum = readFileNum;
        // read file is behind write file and reach the end
        if (readFileNum < writeFileNum && nextReadPosition >= maxBytesPerFileRead) {
            readFile.close();
            readFile = null;

            nextReadFileNum++;
            nextReadPosition = 0;
        }

        return payload;
    }

    private void sync() throws IOException {
        // 1. sync write file but do not close it
        // 2. write metadata to file
        if (writeFile != null) {
            writeFile.getFD().sync();
        }

        writeMetadata();
    }

    public long getSize() {
        try {
            readWriteLock.readLock().lock();
            return size;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public long getMaxBytesPerFile() {
        return maxBytesPerFile;
    }

    public int getSyncInterval() {
        return syncInterval;
    }
}
