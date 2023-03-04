package github.io.pedrogao.diskqueue;

import java.io.Closeable;
import java.io.IOException;

public interface IFanOutQueue extends Closeable {
    /*
     * Constant represents earliest timestamp
     */
    long EARLIEST = -1;
    /*
     * Constant represents latest timestamp
     */
    long LATEST = -2;

    boolean isEmpty(String fanoutId) throws IOException;

    boolean isEmpty();

    long enqueue(byte[] data) throws IOException;

    byte[] dequeue(String fanoutId) throws IOException;

    byte[] peek(String fanoutId) throws IOException;

    int peekLength(String fanoutId) throws IOException;

    long peekTimestamp(String fanoutId) throws IOException;

    byte[] get(long index) throws IOException;

    int getLength(long index) throws IOException;

    long getTimestamp(long index) throws IOException;

    long size(String fanoutId) throws IOException;

    long size();

    void flush();

    void removeBefore(long timestamp) throws IOException;

    void limitBackFileSize(long sizeLimit) throws IOException;

    long getBackFileSize() throws IOException;


    long findClosestIndex(long timestamp) throws IOException;

    void resetQueueFrontIndex(String fanoutId, long index) throws IOException;

    void removeAll() throws IOException;

    long getFrontIndex();

    long getFrontIndex(String fanoutId) throws IOException;

    long getRearIndex();
}
