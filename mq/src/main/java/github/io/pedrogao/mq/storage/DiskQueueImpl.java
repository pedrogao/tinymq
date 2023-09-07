package github.io.pedrogao.mq.storage;

import github.io.pedrogao.diskqueue.FanOutQueueImpl;

import java.io.IOException;

public class DiskQueueImpl implements BackendQueue {

    private final FanOutQueueImpl queue;

    private final String queueName;

    public DiskQueueImpl(String queueDir) throws Exception {
        this(queueDir, "unknown");
    }

    public DiskQueueImpl(String queueDir, String queueName) throws Exception {
        queue = new FanOutQueueImpl(queueDir, queueName);
        this.queueName = queueName;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public long push(byte[] data) throws IOException {
        return queue.enqueue(data);
    }

    @Override
    public byte[] poll(String channel) throws IOException {
        return queue.dequeue(channel);
    }

    @Override
    public byte[] peek(String channel) throws IOException {
        return queue.peek(channel);
    }

    @Override
    public byte[] get(long index) throws IOException {
        return queue.get(index);
    }

    @Override
    public boolean isEmpty(String channel) throws IOException {
        return queue.isEmpty(channel);
    }

    @Override
    public long size(String channel) throws IOException {
        return queue.size(channel);
    }

    @Override
    public void flush() throws IOException {
        queue.flush();
    }

    @Override
    public void close() throws IOException {
        queue.close();
    }

    @Override
    public void delete() throws IOException {
        queue.removeAll();
    }

    @Override
    public void removeBefore(long timestamp) throws IOException {
        queue.removeBefore(timestamp);
    }
}
