package github.io.pedrogao.mq.registry;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;
import github.io.pedrogao.mq.broker.TopicInfo;

import java.io.Closeable;
import java.util.List;

/**
 * Registry Service interface
 */
public interface RegistryService extends Closeable {
    void registerBroker(String brokerId, BrokerInfo brokerInfo);

    void unRegisterBroker(String brokerId);

    BrokerInfo getBroker(String brokerId);

    List<BrokerInfo> getAllBrokers();

    void registerTopic(String topicId, TopicInfo topicInfo);

    void unRegisterTopic(String topicId);

    TopicInfo getTopic(String topicId);

    List<TopicInfo> getAllTopics();

    void registerQueue(String topicId, String queueId, QueueInfo queueInfo);

    void unRegisterQueue(String topicId, String queueId);

    QueueInfo getQueue(String topicId, String queueId);

    void setQueueConsumer(String topicId, String queueId, String consumerId);

    String getQueueConsumer(String topicId, String queueId);

    void unsetQueueConsumer(String topicId, String queueId);

    List<QueueInfo> getQueuesByTopic(String topicId);

    List<QueueInfo> getAllQueues();
}
