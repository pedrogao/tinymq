package github.io.pedrogao.mq.storage;

import java.io.Closeable;
import java.io.IOException;

public interface BackendQueue extends Closeable {
    String getName();

    long push(byte[] data) throws IOException;

    byte[] poll(String channel) throws IOException;

    byte[] peek(String channel) throws IOException;

    byte[] get(long index) throws IOException;

    boolean isEmpty(String channel) throws IOException;

    long size(String channel) throws IOException;

    void flush() throws IOException;

    void close() throws IOException;

    void delete() throws IOException;

    void removeBefore(long timestamp) throws IOException;
}
