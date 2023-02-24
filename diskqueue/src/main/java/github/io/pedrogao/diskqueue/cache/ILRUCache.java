package github.io.pedrogao.diskqueue.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface ILRUCache<K, V extends Closeable> {

    void put(final K key, final V value, final long ttlInMilliSeconds);

    void put(final K key, final V value);

    Optional<V> get(final K key);

    void release(final K key);

    Optional<V> remove(final K key) throws IOException;

    void removeAll() throws IOException;

    int size();

    Collection<V> getValues();
}
