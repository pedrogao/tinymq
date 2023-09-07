package github.io.pedrogao.mq.queue;

import com.google.protobuf.ByteString;
import github.io.pedrogao.mq.api.*;
import github.io.pedrogao.mq.message.MessagePack;
import github.io.pedrogao.mq.registry.RegistryService;
import github.io.pedrogao.mq.storage.BackendQueue;
import github.io.pedrogao.mq.storage.DiskQueueImpl;
import github.io.pedrogao.mq.utils.Closer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueManager extends QueueServiceGrpc.QueueServiceImplBase implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

    private final String dataDir;

    private final Map<String, BackendQueue> queueMap;

    private final ReadWriteLock queueLock;

    private final RegistryService registryService;

    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ScheduledExecutorService cleanExecutor = Executors.newSingleThreadScheduledExecutor();

    private final int flushIntervalMs;
    private final int cleanIntervalMs;
    private final int expireIntervalHour;

    public QueueManager(String dataDir, RegistryService registryService, int flushIntervalMs, int cleanIntervalMs, int expireIntervalHour) {
        this.dataDir = dataDir;
        this.queueMap = new ConcurrentHashMap<>();
        this.queueLock = new ReentrantReadWriteLock();
        this.flushIntervalMs = flushIntervalMs;
        this.cleanIntervalMs = cleanIntervalMs;
        this.expireIntervalHour = expireIntervalHour;
        this.registryService = registryService;

        load();
        flushExecutor.schedule(this::flushQueues, flushIntervalMs, TimeUnit.MILLISECONDS);
        cleanExecutor.schedule(this::cleanupQueueOldItems, cleanIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void produceMessage(ProduceMessageRequest request, StreamObserver<ProduceMessageResponse> responseObserver) {
        String queueName = request.getQueueName();
        ByteString payload = request.getPayload();

        try {
            BackendQueue queue = queueMap.get(queueName);
            if (queue == null) {
                log.error("queue {} not found", queueName);
                Result result = genQueueNotFoundResult(queueName);
                ProduceMessageResponse response = ProduceMessageResponse.newBuilder().setResult(result).build();
                responseObserver.onNext(response);
                return;
            }

            long index = queue.push(payload.toByteArray());
            ProduceMessageResponse response = ProduceMessageResponse.newBuilder().setResult(genSuccessResult()).setIndex(index).build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error("produce message error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }


    @Override
    public void asyncProduceMessage(ProduceMessageRequest request, StreamObserver<Empty> responseObserver) {
        String queueName = request.getQueueName();
        ByteString payload = request.getPayload();
        try {
            BackendQueue queue = queueMap.get(queueName);
            if (queue == null) {
                log.error("queue {} not found", queueName);
                Result result = genQueueNotFoundResult(queueName);
                Empty response = Empty.newBuilder().setResult(result).build();
                responseObserver.onNext(response);
                return;
            }

            // TODO async produce message, no block
            // push message to queue by another background worker thread
            queue.push(payload.toByteArray());
            Result result = genSuccessResult();
            Empty response = Empty.newBuilder().setResult(result).build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error("async produce message error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void consumeMessage(ConsumeMessageRequest request, StreamObserver<ConsumeMessageResponse> responseObserver) {
        String queueName = request.getQueueName();
        String channel = request.getChannelName();
        int batchSize = request.getBatchSize();

        BackendQueue queue = queueMap.get(queueName);
        if (queue == null) {
            log.error("queue {} not found", queueName);

            Result result = genQueueNotFoundResult(queueName);
            ConsumeMessageResponse response = ConsumeMessageResponse.newBuilder().setResult(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        try {
            MessageList.Builder builder = MessageList.newBuilder();
            MessagePack.Builder messagePackBuilder = MessagePack.newBuilder();
            int size = 0;
            while (batchSize > 0) {
                byte[] item = queue.poll(channel);
                if (item == null) {
                    break;
                }

                messagePackBuilder.addPayloads(ByteString.copyFrom(item));
                batchSize--;
                size++;
            }

            MessagePack messagePack = messagePackBuilder.build();
            byte[] bytes = messagePack.toByteArray();

            builder.setSize(size);
            builder.setCompressionType(CompressionType.NONE); // TODO
            builder.setPayload(ByteString.copyFrom(bytes));

            Result result = genSuccessResult();
            ConsumeMessageResponse response = ConsumeMessageResponse.newBuilder().setResult(result).setMessageList(builder.build()).build();
            responseObserver.onNext(response);
        } catch (IOException e) {
            log.error("poll message error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void fetchMessage(FetchMessageRequest request, StreamObserver<FetchMessageResponse> responseObserver) {
        String queueName = request.getQueueName();
        String channelName = request.getChannelName(); // TODO 暂时没啥用
        long index = request.getIndex();
        int batchSize = request.getBatchSize();

        try {
            BackendQueue queue = queueMap.get(queueName);
            if (queue == null) {
                log.error("queue {} not found", queueName);
                Result result = genQueueNotFoundResult(queueName);
                FetchMessageResponse response = FetchMessageResponse.newBuilder().setResult(result).build();
                responseObserver.onNext(response);
                return;
            }

            MessageList.Builder builder = MessageList.newBuilder();
            MessagePack.Builder messagePackBuilder = MessagePack.newBuilder();
            int size = 0;
            while (batchSize > 0) {
                byte[] item = queue.get(index);
                if (item == null) {
                    break;
                }

                messagePackBuilder.addPayloads(ByteString.copyFrom(item));
                batchSize--;
                size++;
                index++;
            }

            MessagePack messagePack = messagePackBuilder.build();
            byte[] bytes = messagePack.toByteArray();
            builder.setSize(size);
            builder.setCompressionType(CompressionType.NONE); // TODO
            builder.setPayload(ByteString.copyFrom(bytes));

            Result result = genSuccessResult();
            FetchMessageResponse response = FetchMessageResponse.newBuilder().setResult(result).setMessageList(builder.build()).build();
            responseObserver.onNext(response);
        } catch (IOException e) {
            log.error("fetch message error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createQueue(CreateQueueRequest request, StreamObserver<CreateQueueResponse> responseObserver) {
        String queueName = request.getQueueName();

        try {
            queueLock.writeLock().lock();
            log.info("create queue {} in {}", queueName, dataDir);
            // Support call many times
            queueMap.putIfAbsent(queueName, new DiskQueueImpl(dataDir + File.separator + queueName, queueName));
            CreateQueueResponse response = CreateQueueResponse.newBuilder().setResult(genSuccessResult()).build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error("create queue error", e);
            responseObserver.onError(e);
        } finally {
            queueLock.writeLock().unlock();
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteQueue(DeleteQueueRequest request, StreamObserver<DeleteQueueResponse> responseObserver) {
        try {
            queueLock.writeLock().lock();

            String queueName = request.getQueueName();
            BackendQueue queue = queueMap.get(queueName);
            if (queue == null) {
                Result result = genQueueNotFoundResult(queueName);
                DeleteQueueResponse response = DeleteQueueResponse.newBuilder().setResult(result).build();
                responseObserver.onNext(response);
                return;
            }

            queue.delete();
            Result result = genSuccessResult();
            DeleteQueueResponse response = DeleteQueueResponse.newBuilder().setResult(result).build();
            responseObserver.onNext(response);
        } catch (IOException e) {
            log.error("delete queue error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
            queueLock.writeLock().unlock();
        }
    }

    @Override
    public void getQueueSize(GetQueueSizeRequest request, StreamObserver<GetQueueSizeResponse> responseObserver) {
        String queueName = request.getQueueName();
        String channelName = request.getChannelName();
        BackendQueue queue = queueMap.get(queueName);
        if (queue == null) {
            log.error("queue {} not found", queueName);

            Result result = genQueueNotFoundResult(queueName);
            GetQueueSizeResponse response = GetQueueSizeResponse.newBuilder().setResult(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        try {
            long size = queue.size(channelName);
            Result result = genSuccessResult();
            GetQueueSizeResponse response = GetQueueSizeResponse.newBuilder().setResult(result).setSize(size).build();
            responseObserver.onNext(response);
        } catch (IOException e) {
            log.error("get queue size error", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void close() throws IOException {
        log.info("close queue manager");
        for (BackendQueue queue : queueMap.values()) {
            Closer.closeQuietly(queue, log);
        }
        queueMap.clear();
        flushExecutor.shutdown();
        cleanExecutor.shutdown();
    }

    private void cleanupQueueOldItems() {
        // remove items older
        long startMs = System.currentTimeMillis();
        long removedMs = startMs - TimeUnit.HOURS.toMillis(expireIntervalHour);

        for (BackendQueue queue : queueMap.values()) {
            try {
                queue.removeBefore(removedMs);
            } catch (IOException e) {
                log.error("cleanup queue {} error", queue.getName(), e);
            }
        }
    }

    private void flushQueues() {
        for (BackendQueue queue : queueMap.values()) {
            try {
                queue.flush();
            } catch (IOException e) {
                log.error("flush queue {} error", queue.getName(), e);
            }
        }
    }

    private void load() {
        File file = new File(dataDir);
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("dataDir " + dataDir + " is not a directory");
        }

        if (!file.exists()) {
            file.mkdirs();
        }

        if (!file.canRead()) {
            throw new IllegalArgumentException("dataDir " + dataDir + " can not read");
        }

        File[] subDirs = file.listFiles();
        if (subDirs == null) {
            return;
        }

        for (File subDir : subDirs) {
            String queueName = subDir.getName();
            try {
                BackendQueue queue = new DiskQueueImpl(subDir.getAbsolutePath(), queueName);
                queueMap.put(queueName, queue);
            } catch (Exception e) {
                log.error("load queue {} error", queueName, e);
            }
        }
    }

    private Result genSuccessResult() {
        return Result.newBuilder().setResultCode(ResultCode.forNumber(ResultCode.SUCCESS_VALUE)).build();
    }

    private Result genQueueNotFoundResult(String queueName) {
        return Result.newBuilder().setResultCode(ResultCode.forNumber(ResultCode.FAILURE_VALUE)).
                setErrorCode(ErrorCode.QUEUE_NOT_FOUND).setErrorMessage("queue " + queueName + " not found").build();
    }
}
