package github.io.pedrogao.mq.producer;

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
import java.util.stream.Collectors;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final ProducerConfig config;

    private final RegistryService registryService;

    private final String id;

    public Producer(ProducerConfig config) {
        this.config = config;
        this.registryService = new ZookeeperRegistry(config.getZkAddress());
        this.id = UUID.randomUUID().toString();
    }

    private BrokerInfo getBrokerInfo(String brokerId) {
        return registryService.getBroker(brokerId);
    }

    private List<BrokerInfo> getBrokerInfoList() {
        return registryService.getAllBrokers();
    }

    private TopicInfo getTopicInfo(String topic) {
        return registryService.getTopic(topic);
    }

    private List<QueueInfo> getTopicQueueInfoList(String topic) {
        return registryService.getQueuesByTopic(topic);
    }

    public SenderPool getSenderPool(String topic) {
        TopicInfo topicInfo = getTopicInfo(topic);
        if (topicInfo == null) {
            return null;
        }
        log.debug("Get topic info {}", topicInfo);

        IPartitioner partitioner = PartitionerFactory.getPartitioner(config.getPartitionType());

        List<QueueInfo> queueInfoList = getTopicQueueInfoList(topic);
        if (queueInfoList == null || queueInfoList.isEmpty())
            throw new QueueException("No queue found for topic " + topic);

        List<BrokerInfo> brokerInfoList = queueInfoList.stream().
                map(QueueInfo::getBrokerId).
                map(registryService::getBroker).
                collect(Collectors.toList());

        return new SenderPool(brokerInfoList, queueInfoList, partitioner);
    }
}
