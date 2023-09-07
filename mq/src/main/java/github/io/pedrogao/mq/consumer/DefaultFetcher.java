package github.io.pedrogao.mq.consumer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import github.io.pedrogao.mq.api.CompressionType;
import github.io.pedrogao.mq.api.MessageList;
import github.io.pedrogao.mq.client.AbstractApiClient;
import github.io.pedrogao.mq.message.Message;
import github.io.pedrogao.mq.message.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultFetcher extends AbstractApiClient implements IFetcher {

    private static final Logger log = LoggerFactory.getLogger(DefaultFetcher.class);

    private final String queue;

    private final String channel;

    private final int batchSize;

    public DefaultFetcher(String host, int port, String queue, String channel, int batchSize) {
        super(host, port);
        this.queue = queue;
        this.channel = channel;
        this.batchSize = batchSize;
    }

    @Override
    public List<Message> fetch() {
        return fetchOneQueue(queue, batchSize);
    }

    private List<Message> fetchOneQueue(String queueName, int batchSize) {
        MessageList messageList = consumeMessage(queueName, channel, batchSize);
        if (messageList.getSize() == 0) {
            return new ArrayList<>();
        }

        CompressionType compressionType = messageList.getCompressionType(); // TODO

        ByteString payload = messageList.getPayload();

        MessagePack messagePack = null;
        try {
            messagePack = MessagePack.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            log.error("parse message pack error", e);
            throw new RuntimeException(e);
        }

        List<ByteString> payloadsList = messagePack.getPayloadsList();
        if (payloadsList.size() == 0) {
            return new ArrayList<>();
        }

        List<Message> messages = new ArrayList<>();
        for (ByteString bs : payloadsList) {
            Message message = Message.deserialize(bs.toByteArray());
            messages.add(message);
        }

        return messages;
    }
}
