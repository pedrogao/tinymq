package github.io.pedrogao.mq.client;

import com.google.protobuf.ByteString;
import github.io.pedrogao.mq.api.*;
import github.io.pedrogao.mq.exception.StubException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class AbstractApiClient {

    private static final Logger log = LoggerFactory.getLogger(AbstractApiClient.class);

    private final ManagedChannel channel;
    private final QueueServiceGrpc.QueueServiceBlockingStub blockingStub;

    private volatile boolean shutdown = false;

    public AbstractApiClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = QueueServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        if (shutdown)
            return;

        log.info("shutdown client");
        while (!shutdown) {
            shutdown = channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public long produceMessage(String queueName, byte[] payload) {
        ProduceMessageRequest request = ProduceMessageRequest.newBuilder().
                setQueueName(queueName).setPayload(ByteString.copyFrom(payload)).build();
        ProduceMessageResponse response = blockingStub.produceMessage(request);

        Result result = response.getResult();
        throwIfNotSuccess(result);

        return response.getIndex();
    }

    public void asyncProduceMessage(String queueName, byte[] payload) {
        ProduceMessageRequest request = ProduceMessageRequest.newBuilder().
                setQueueName(queueName).setPayload(ByteString.copyFrom(payload)).build();
        Empty response = blockingStub.asyncProduceMessage(request);
        Result result = response.getResult();
        throwIfNotSuccess(result);
    }

    public MessageList consumeMessage(String queueName, String channelName, int batchSize) {
        ConsumeMessageRequest request = ConsumeMessageRequest.newBuilder().
                setQueueName(queueName).setChannelName(channelName).setBatchSize(batchSize).build();
        ConsumeMessageResponse response = blockingStub.consumeMessage(request);

        Result result = response.getResult();
        throwIfNotSuccess(result);

        return response.getMessageList();
    }

    public MessageList fetchMessage(String queueName, String channelName, int index, int batchSize) {
        FetchMessageRequest request = FetchMessageRequest.newBuilder().
                setQueueName(queueName).setChannelName(channelName).
                setIndex(index).setBatchSize(batchSize).build();

        FetchMessageResponse response = blockingStub.fetchMessage(request);
        Result result = response.getResult();
        throwIfNotSuccess(result);

        return response.getMessageList();
    }

    public void createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.newBuilder().setQueueName(queueName).build();
        CreateQueueResponse response = blockingStub.createQueue(request);
        Result result = response.getResult();
        throwIfNotSuccess(result);
    }

    public void deleteQueue(String queueName) {
        DeleteQueueRequest request = DeleteQueueRequest.newBuilder().setQueueName(queueName).build();
        DeleteQueueResponse response = blockingStub.deleteQueue(request);
        Result result = response.getResult();
        throwIfNotSuccess(result);
    }

    public long getQueueSize(String queueName, String channelName) {
        GetQueueSizeResponse response = blockingStub.getQueueSize(GetQueueSizeRequest.newBuilder().
                setQueueName(queueName).setChannelName(channelName).build());

        Result result = response.getResult();
        throwIfNotSuccess(result);

        return response.getSize();
    }

    private void throwIfNotSuccess(Result result) {
        if (result.getResultCodeValue() != ResultCode.SUCCESS_VALUE) {
            log.error("error code: {}, error message: {}", result.getErrorCodeValue(), result.getErrorMessage());
            throw new StubException(result.getErrorMessage(), result.getErrorCodeValue());
        }
    }
}
