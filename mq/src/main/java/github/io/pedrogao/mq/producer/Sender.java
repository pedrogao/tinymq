package github.io.pedrogao.mq.producer;

import github.io.pedrogao.mq.client.AbstractApiClient;
import github.io.pedrogao.mq.message.Message;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Sender extends AbstractApiClient implements ISender {

    private final String queue;

    public Sender(String host, int port, String queue) {
        super(host, port);
        this.queue = queue;
    }

    @Override
    public void send(byte[] payload) {
        byte[] id = generateId();

        Message message = new Message(id, payload);
        produceMessage(queue, message.serialize());
    }

    @Override
    public void send(String payload) {
        send(payload.getBytes());
    }

    private byte[] generateId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
