package github.io.pedrogao.diskqueue;

import java.io.Closeable;
import java.io.IOException;

public interface IBigArray extends Closeable {
    long NOT_FOUND = -1;

    long append(byte[] data) throws IOException;

    byte[] get(long index) throws IOException;

    long getTimestamp(long index) throws IOException;

    long size();

    int getDataPageSize();

    long getHeadIndex();

    long getTailIndex();

    boolean isEmpty();

    boolean isFull();

    void removeAll() throws IOException;

    void removeBeforeIndex(long index) throws IOException;

    void removeBefore(long timestamp) throws IOException;

    void flush();

    long findClosestIndex(long timestamp) throws IOException;

    long getBackFileSize() throws IOException;

    void limitBackFileSize(long sizeLimit) throws IOException;

    int getItemLength(long index) throws IOException;
}
