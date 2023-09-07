package github.io.pedrogao.mq.consumer;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;
import github.io.pedrogao.mq.broker.TopicInfo;
import github.io.pedrogao.mq.exception.QueueException;
import github.io.pedrogao.mq.registry.RegistryService;
import github.io.pedrogao.mq.registry.ZookeeperRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final ConsumerConfig config;

    private final RegistryService registryService;

    private final String id;

    public Consumer(ConsumerConfig config) {
        this.config = config;
        this.registryService = new ZookeeperRegistry(config.getZkAddress());
        this.id = UUID.randomUUID().toString();
    }

    private BrokerInfo getBrokerInfo(String brokerId) {
        return registryService.getBroker(brokerId);
    }

    private TopicInfo getTopicInfo(String topic) {
        return registryService.getTopic(topic);
    }

    private List<QueueInfo> getTopicQueueInfoList(String topic) {
        return registryService.getQueuesByTopic(topic);
    }

    public MessageStream getMessageStream(String topic, String channel) {
        // 1. Choose a queue to consume randomly
        // 2. Update QueueInfo's consumerId by ZK
        // 3. Return MessageStream
        TopicInfo topicInfo = getTopicInfo(topic);
        if (topicInfo == null) {
            return null;
        }
        log.debug("Get topic info {}", topicInfo);

        List<QueueInfo> queueInfoList = getTopicQueueInfoList(topic);

        QueueInfo nextQueueInfo = null;
        for (QueueInfo queueInfo : queueInfoList) {
            String consumerId = registryService.getQueueConsumer(topic, queueInfo.getId());
            if (consumerId == null) {
                nextQueueInfo = queueInfo;
                break;
            }
        }
        if (nextQueueInfo == null) {
            throw new QueueException("can't find avilable queue in topic " + topic);
        }

        registryService.setQueueConsumer(topic, nextQueueInfo.getId(), id); // set current consumer id
        String brokerId = nextQueueInfo.getBrokerId();
        BrokerInfo brokerInfo = getBrokerInfo(brokerId);

        String host = brokerInfo.getHost();
        int port = brokerInfo.getPort();
        String queue = nextQueueInfo.getId();
        int batchSize = config.getBatchSize();
        return new MessageStream(host, port, queue, channel, batchSize);
    }
}
