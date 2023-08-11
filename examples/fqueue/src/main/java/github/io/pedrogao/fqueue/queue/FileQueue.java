package github.io.pedrogao.fqueue.queue;

import github.io.pedrogao.fqueue.exception.FileEOFException;
import github.io.pedrogao.fqueue.exception.FileFormatException;
import github.io.pedrogao.fqueue.log.HeapFile;
import github.io.pedrogao.fqueue.log.IndexFile;

import java.io.File;
import java.io.IOException;

import static github.io.pedrogao.fqueue.log.Const.*;

public class FileQueue {
    private long limitLength;

    private String path;

    private IndexFile indexFile;

    private HeapFile writeHeapFile;

    private HeapFile readHeapFile;

    private long readerIndex = -1;

    private long writerIndex = -1;

    public FileQueue(File dir, long limitLength) throws IOException, FileFormatException {
        if (!dir.exists() && !dir.isDirectory()) {
            if (!dir.mkdirs()) {
                throw new IOException("create dir error");
            }
        }
        this.limitLength = limitLength;
        this.path = dir.getAbsolutePath();

        indexFile = new IndexFile(path);
        init();
    }

    public FileQueue(String path) {
        // TODO
    }

    public void add(String message) throws FileFormatException, IOException {
        add(message.getBytes());
    }

    public void add(byte[] message) throws FileFormatException, IOException {
        byte status = writeHeapFile.write(message);
        if (status == WRITE_FULL) {
            rotateNextLogWriter();
            status = writeHeapFile.write(message);
        }
        if (status == WRITE_SUCCESS) {
            indexFile.incrementSize();
        }
    }

    public byte[] readNext() throws FileFormatException, IOException {
        return read(false);
    }

    public byte[] readNextAndRemove() throws FileFormatException, IOException {
        return read(true);
    }

    private byte[] read(boolean commit) throws IOException, FileFormatException {
        byte[] bytes;
        try {
            bytes = readHeapFile.read(commit);
        } catch (FileEOFException e) {
            long nextFileNumber = readHeapFile.getNextFileNumber();
            readHeapFile.reset();
            File deleteFile = readHeapFile.getFile();
            readHeapFile.close();
            deleteFile.delete();

            // update next read file
            indexFile.putReaderPosition(HEAP_HEAD_LENGTH);
            indexFile.putReaderIndex(nextFileNumber);
            if (writeHeapFile.getCurrentFileNumber() == nextFileNumber) {
                readHeapFile = writeHeapFile;
            } else {
                readHeapFile = new HeapFile(path, nextFileNumber, limitLength, indexFile);
            }

            try {
                bytes = readHeapFile.read(commit);
            } catch (FileEOFException e1) {
                throw new FileFormatException(e1);
            }
        }
        if (bytes != null && commit) {
            indexFile.decrementSize();
        }
        return bytes;
    }

    private void init() throws FileFormatException, IOException {
        writerIndex = indexFile.getWriterIndex();
        readerIndex = indexFile.getReaderIndex();

        writeHeapFile = new HeapFile(path, writerIndex, limitLength, indexFile);
        if (readerIndex == writerIndex) {
            readHeapFile = writeHeapFile;
        } else {
            readHeapFile = new HeapFile(path, readerIndex, limitLength, indexFile);
        }
    }

    private void rotateNextLogWriter() throws IOException, FileFormatException {
        writerIndex++;
        writeHeapFile.putNextFileNumber(writerIndex);
        if (readHeapFile != writeHeapFile) {
            writeHeapFile.close();
        }

        indexFile.putWriterIndex(writerIndex);
        writeHeapFile = new HeapFile(path, writerIndex, limitLength, indexFile, true);
    }

    public void clear() throws IOException, FileFormatException {
        indexFile.clear();
        init();
    }

    public void close() throws IOException {
        readHeapFile.close();
        writeHeapFile.close();
        indexFile.close();
    }

    public long getQueueSize() {
        return indexFile.getSize();
    }
}
