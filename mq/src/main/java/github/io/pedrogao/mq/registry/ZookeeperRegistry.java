package github.io.pedrogao.mq.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;
import github.io.pedrogao.mq.broker.TopicInfo;
import github.io.pedrogao.mq.registry.zookeeper.CuratorZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZookeeperRegistry implements RegistryService {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperRegistry.class);

    public static final String ROOT_PATH = "/tinymq";

    public static final String BROKER_PATH = ROOT_PATH + "/broker";

    public static final String TOPIC_PATH = ROOT_PATH + "/topic";

    private final CuratorZookeeperClient zookeeperClient;

    private final ObjectMapper objectMapper;

    public ZookeeperRegistry(String zkAddress) {
        zookeeperClient = new CuratorZookeeperClient(zkAddress);
        objectMapper = new ObjectMapper();
    }

    @Override
    public void registerBroker(String brokerId, BrokerInfo brokerInfo) {
        String brokerPath = BROKER_PATH + "/" + brokerId;
        String data = serialize(brokerInfo);
        // broker info is temporary data, if broker disconnect, the data will be deleted
        zookeeperClient.createTemporaryData(brokerPath, data);
    }

    @Override
    public void unRegisterBroker(String brokerId) {
        String brokerPath = BROKER_PATH + "/" + brokerId;
        zookeeperClient.deleteNode(brokerPath);
    }

    @Override
    public BrokerInfo getBroker(String brokerId) {
        String brokerPath = BROKER_PATH + "/" + brokerId;
        String data = zookeeperClient.getNodeData(brokerPath);
        if (data == null || data.isEmpty())
            return null;
        return deserialize(data, BrokerInfo.class);
    }

    @Override
    public List<BrokerInfo> getAllBrokers() {
        List<String> children = zookeeperClient.getChildrenData(BROKER_PATH);
        if (children == null || children.isEmpty())
            return new ArrayList<>();

        List<BrokerInfo> brokerInfoList = new ArrayList<>();
        for (String child : children) {
            String brokerPath = BROKER_PATH + "/" + child;
            String data = zookeeperClient.getNodeData(brokerPath);
            if (data == null || data.isEmpty())
                continue;
            BrokerInfo brokerInfo = deserialize(data, BrokerInfo.class);
            brokerInfoList.add(brokerInfo);
        }
        return brokerInfoList;
    }

    @Override
    public void registerTopic(String topicId, TopicInfo topicInfo) {
        String topicPath = TOPIC_PATH + "/" + topicId;
        String data = serialize(topicInfo);
        zookeeperClient.createPersistentData(topicPath, data);
    }

    @Override
    public void unRegisterTopic(String topicId) {
        String topicPath = TOPIC_PATH + "/" + topicId;
        zookeeperClient.deleteNode(topicPath);
    }

    @Override
    public TopicInfo getTopic(String topicId) {
        String topicPath = TOPIC_PATH + "/" + topicId;
        String data = zookeeperClient.getNodeData(topicPath);
        if (data == null || data.isEmpty())
            return null;
        return deserialize(data, TopicInfo.class);
    }

    @Override
    public List<TopicInfo> getAllTopics() {
        List<String> children = zookeeperClient.getChildrenData(TOPIC_PATH);
        if (children == null || children.isEmpty())
            return new ArrayList<>();

        List<TopicInfo> topicInfoList = new ArrayList<>();
        for (String child : children) {
            String topicPath = TOPIC_PATH + "/" + child;
            String data = zookeeperClient.getNodeData(topicPath);
            if (data == null || data.isEmpty())
                continue;
            TopicInfo topicInfo = deserialize(data, TopicInfo.class);
            topicInfoList.add(topicInfo);
        }
        return topicInfoList;
    }

    @Override
    public void registerQueue(String topicId, String queueId, QueueInfo queueInfo) {
        String queuePath = TOPIC_PATH + "/" + topicId + "/" + queueId;
        String data = serialize(queueInfo);
        zookeeperClient.createPersistentData(queuePath, data);
    }

    @Override
    public void unRegisterQueue(String topicId, String queueId) {
        String queuePath = TOPIC_PATH + "/" + topicId + "/" + queueId;
        zookeeperClient.deleteNode(queuePath);
    }

    @Override
    public QueueInfo getQueue(String topicId, String queueId) {
        String queuePath = TOPIC_PATH + "/" + topicId + "/" + queueId;
        String data = zookeeperClient.getNodeData(queuePath);
        if (data == null || data.isEmpty())
            return null;
        return deserialize(data, QueueInfo.class);
    }

    @Override
    public void setQueueConsumer(String topicId, String queueId, String consumerId) {
        String queueConsumerPath = TOPIC_PATH + "/" + topicId + "/" + queueId + "/consume";
        // Create temporary node, deleted when consumer disconnected
        zookeeperClient.createTemporaryData(queueConsumerPath, consumerId);
    }

    @Override
    public String getQueueConsumer(String topicId, String queueId) {
        String queueConsumerPath = TOPIC_PATH + "/" + topicId + "/" + queueId + "/consume";
        return zookeeperClient.getNodeData(queueConsumerPath);
    }

    @Override
    public void unsetQueueConsumer(String topicId, String queueId) {
        String queueConsumerPath = TOPIC_PATH + "/" + topicId + "/" + queueId + "/consume";
        zookeeperClient.deleteNode(queueConsumerPath);
    }

    @Override
    public List<QueueInfo> getQueuesByTopic(String topicId) {
        String topicPath = TOPIC_PATH + "/" + topicId;
        List<String> children = zookeeperClient.getChildrenData(topicPath);

        if (children == null || children.isEmpty())
            return new ArrayList<>();

        List<QueueInfo> queueInfoList = new ArrayList<>();
        for (String child : children) {
            String queuePath = TOPIC_PATH + "/" + topicId + "/" + child;
            String data = zookeeperClient.getNodeData(queuePath);
            if (data == null || data.isEmpty())
                continue;
            QueueInfo queueInfo = deserialize(data, QueueInfo.class);
            queueInfoList.add(queueInfo);
        }
        return queueInfoList;
    }

    @Override
    public List<QueueInfo> getAllQueues() {
        List<String> children = zookeeperClient.getChildrenData(TOPIC_PATH);
        if (children == null || children.isEmpty())
            return new ArrayList<>();

        List<QueueInfo> queueInfoList = new ArrayList<>();
        for (String child : children) {
            queueInfoList.addAll(getQueuesByTopic(child));
        }
        return queueInfoList;
    }

    private String serialize(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.error("serialize object error", e);
            throw new RuntimeException(e);
        }
    }

    private <T> T deserialize(String data, Class<T> classType) {
        try {
            return objectMapper.readValue(data, classType);
        } catch (JsonProcessingException e) {
            log.error("deserialize object error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        zookeeperClient.destroy();
    }

    protected CuratorZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }
}
