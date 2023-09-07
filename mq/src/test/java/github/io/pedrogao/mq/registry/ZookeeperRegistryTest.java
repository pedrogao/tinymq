package github.io.pedrogao.mq.registry;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;
import github.io.pedrogao.mq.broker.TopicInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class ZookeeperRegistryTest {

    private static ZookeeperRegistry zookeeperRegistry;

    private final Random random = new Random(System.currentTimeMillis());


    @BeforeAll
    static void setup() {
        zookeeperRegistry = new ZookeeperRegistry("localhost:2181");
    }

    @AfterAll
    static void down() throws IOException {
        zookeeperRegistry.close();
    }

    @Test
    void registerBroker() {
        String brokerId = "Broker" + random.nextInt();
        BrokerInfo brokerInfo = new BrokerInfo(brokerId, "127.0.0.1", 7070);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);

        BrokerInfo broker = zookeeperRegistry.getBroker(brokerId);
        assertEquals(broker.getPort(), 7070);
        assertEquals(broker.getId(), brokerId);

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.BROKER_PATH + "/" + brokerId);
    }

    @Test
    void unRegisterBroker() {
        String brokerId = "Broker" + random.nextInt();
        BrokerInfo brokerInfo = new BrokerInfo(brokerId, "127.0.0.1", 7070);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);

        BrokerInfo broker = zookeeperRegistry.getBroker(brokerId);
        assertEquals(broker.getPort(), 7070);
        assertEquals(broker.getId(), brokerId);

        zookeeperRegistry.unRegisterBroker(brokerId);
        broker = zookeeperRegistry.getBroker(brokerId);
        assertNull(broker);
    }

    @Test
    void getAllBrokers() {
        String brokerId = "Broker" + random.nextInt();
        BrokerInfo brokerInfo = new BrokerInfo(brokerId, "127.0.0.1", 7070);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);
        zookeeperRegistry.registerBroker(brokerId, brokerInfo);

        List<BrokerInfo> brokerInfoList = zookeeperRegistry.getAllBrokers();
        assertEquals(brokerInfoList.size(), 1);
        var broker = brokerInfoList.get(0);
        assertEquals(broker.getPort(), 7070);
        assertEquals(broker.getId(), brokerId);

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.BROKER_PATH + "/" + brokerId);
    }

    @Test
    void registerTopic() {
        String topicId = "Topic" + random.nextInt();
        TopicInfo topicInfo = new TopicInfo(topicId, Arrays.asList("queue1", "queue2"), 1);
        zookeeperRegistry.registerTopic(topicId, topicInfo);

        TopicInfo broker = zookeeperRegistry.getTopic(topicId);
        assertEquals(broker.getNumPartitions(), 1);
        assertEquals(broker.getId(), topicId);
        assertEquals(broker.getQueueIds().size(), 2);

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.TOPIC_PATH + "/" + topicId);
    }

    @Test
    void unRegisterTopic() {
        String topicId = "Topic" + random.nextInt();
        TopicInfo topicInfo = new TopicInfo(topicId, Arrays.asList("queue1", "queue2"), 1);
        zookeeperRegistry.registerTopic(topicId, topicInfo);

        TopicInfo topic = zookeeperRegistry.getTopic(topicId);
        assertEquals(topic.getNumPartitions(), 1);
        assertEquals(topic.getId(), topicId);
        assertEquals(topic.getQueueIds().size(), 2);

        zookeeperRegistry.unRegisterTopic(topicId);
        topic = zookeeperRegistry.getTopic(topicId);
        assertNull(topic);
    }

    @Test
    void getAllTopics() {
        String topicId = "Topic" + random.nextInt();
        TopicInfo topicInfo = new TopicInfo(topicId, Arrays.asList("queue1", "queue2"), 1);
        zookeeperRegistry.registerTopic(topicId, topicInfo);

        var topicInfoList = zookeeperRegistry.getAllTopics();
        assertEquals(topicInfoList.size(), 1);
        var topic = topicInfoList.get(0);
        assertEquals(topic.getNumPartitions(), 1);
        assertEquals(topic.getId(), topicId);
        assertEquals(topic.getQueueIds().size(), 2);

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.TOPIC_PATH + "/" + topicId);
    }

    @Test
    void registerQueue() {
        String topicId = "Topic" + random.nextInt();
        String queueId = "Queue" + random.nextInt();
        QueueInfo queueInfo = new QueueInfo(queueId, "broker1");
        zookeeperRegistry.registerQueue(topicId, queueId, queueInfo);

        QueueInfo queue = zookeeperRegistry.getQueue(topicId, queueId);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.TOPIC_PATH + "/" + topicId + "/" + queueId);
    }

    @Test
    void unRegisterQueue() {
        String topicId = "Topic" + random.nextInt();
        String queueId = "Queue" + random.nextInt();
        QueueInfo queueInfo = new QueueInfo(queueId, "broker1");
        zookeeperRegistry.registerQueue(topicId, queueId, queueInfo);
        zookeeperRegistry.registerQueue(topicId, queueId, queueInfo);

        QueueInfo queue = zookeeperRegistry.getQueue(topicId, queueId);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        zookeeperRegistry.unRegisterQueue(topicId, queueId);
        queue = zookeeperRegistry.getQueue(topicId, queueId);
        assertNull(queue);
    }

    @Test
    void setQueueConsumer() {
        String topicId = "Topic" + random.nextInt();
        String queueId = "Queue" + random.nextInt();
        String consumerId = "pedrogao";
        zookeeperRegistry.setQueueConsumer(topicId, queueId, consumerId);
        zookeeperRegistry.setQueueConsumer(topicId, queueId, consumerId);

        var ret = zookeeperRegistry.getQueueConsumer(topicId, queueId);
        assertEquals(ret, consumerId);

        zookeeperRegistry.unsetQueueConsumer(topicId, queueId);
        ret = zookeeperRegistry.getQueueConsumer(topicId, queueId);
        assertNull(ret);
    }

    @Test
    void getQueuesByTopic() {
        String topicId = "Topic" + random.nextInt();
        String queueId = "Queue" + random.nextInt();
        QueueInfo queueInfo = new QueueInfo(queueId, "broker1");
        zookeeperRegistry.registerQueue(topicId, queueId, queueInfo);

        QueueInfo queue = zookeeperRegistry.getQueue(topicId, queueId);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        List<QueueInfo> allQueues = zookeeperRegistry.getQueuesByTopic(topicId);
        assertEquals(allQueues.size(), 1);
        queue = allQueues.get(0);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.TOPIC_PATH + "/" + topicId + "/" + queueId);
    }

    @Test
    void getAllQueues() {
        String topicId = "Topic" + random.nextInt();
        String queueId = "Queue" + random.nextInt();
        QueueInfo queueInfo = new QueueInfo(queueId, "broker1");
        zookeeperRegistry.registerQueue(topicId, queueId, queueInfo);

        QueueInfo queue = zookeeperRegistry.getQueue(topicId, queueId);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        List<QueueInfo> allQueues = zookeeperRegistry.getAllQueues();
        assertEquals(allQueues.size(), 1);
        queue = allQueues.get(0);
        assertEquals(queue.getId(), queueId);
        assertEquals(queue.getBrokerId(), "broker1");

        zookeeperRegistry.getZookeeperClient().deleteNode(ZookeeperRegistry.TOPIC_PATH + "/" + topicId + "/" + queueId);
    }
}