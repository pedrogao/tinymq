package github.io.pedrogao.diskqueue;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;

public interface IBigQueue extends Closeable {

    boolean isEmpty();

    void enqueue(byte[] data) throws IOException;

    byte[] dequeue() throws IOException;

    ListenableFuture<byte[]> dequeueAsync();

    void removeAll() throws IOException;

    byte[] peek() throws IOException;

    ListenableFuture<byte[]> peekAsync();

    void applyForEach(ItemIterator iterator) throws IOException;

    void gc() throws IOException;

    void flush();

    long size();

    interface ItemIterator {
        void forEach(byte[] item) throws IOException;
    }
}
