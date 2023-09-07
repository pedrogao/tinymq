package github.io.pedrogao.mq.admin;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;
import github.io.pedrogao.mq.broker.TopicInfo;
import github.io.pedrogao.mq.client.AbstractApiClient;
import github.io.pedrogao.mq.registry.RegistryService;
import github.io.pedrogao.mq.registry.ZookeeperRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdminManager implements AutoCloseable {

    private String zkAddress;

    private final Map<String, AdminClient> adminClientMap = new ConcurrentHashMap<>();

    private List<BrokerInfo> brokerList;

    private RegistryService registryService;

    public AdminManager(String zkAddress) {
        this.zkAddress = zkAddress;
        this.registryService = new ZookeeperRegistry(zkAddress);

        buildAdminClientMap();
    }

    private void buildAdminClientMap() {
        List<BrokerInfo> allBrokers = registryService.getAllBrokers();
        for (BrokerInfo brokerInfo : allBrokers) {
            AdminClient adminClient = new AdminClient(brokerInfo.getHost(), brokerInfo.getPort());
            adminClientMap.put(brokerInfo.getId(), adminClient);
        }
        brokerList = allBrokers;
    }

    public void createTopic(String topic, int partitionNum) {
        if (partitionNum <= 0) {
            throw new IllegalArgumentException("Partition number must be greater than 0");
        }

        if (partitionNum > brokerList.size()) {
            throw new IllegalArgumentException("Partition number must be less than or equal to broker number");
        }

        if (registryService.getTopic(topic) != null) {
            throw new IllegalArgumentException("Topic " + topic + " already exists");
        }

        List<String> queueIdList = new ArrayList<>();
        List<QueueInfo> queueInfoList = new ArrayList<>();
        for (int i = 0; i < partitionNum; i++) {
            BrokerInfo brokerInfo = brokerList.get(i);
            String queueId = topic + "-" + i;
            queueIdList.add(queueId);
            QueueInfo queueInfo = new QueueInfo(queueId, brokerInfo.getId());
            queueInfoList.add(queueInfo);
        }

        TopicInfo topicInfo = new TopicInfo(topic, queueIdList, partitionNum);
        // Register topic
        registryService.registerTopic(topic, topicInfo);
        // Register queues
        for (QueueInfo queueInfo : queueInfoList) {
            registryService.registerQueue(topic, queueInfo.getId(), queueInfo);
            adminClientMap.get(queueInfo.getBrokerId()).createQueue(queueInfo.getId());
        }
    }

    public void deleteTopic(String topic) {
        if (registryService.getTopic(topic) == null) {
            throw new IllegalArgumentException("Topic " + topic + " not exists");
        }

        List<QueueInfo> queueInfoList = registryService.getQueuesByTopic(topic);
        for (QueueInfo queueInfo : queueInfoList) {
            registryService.unRegisterQueue(topic, queueInfo.getId());
            adminClientMap.get(queueInfo.getBrokerId()).deleteQueue(queueInfo.getId());
        }
        registryService.unRegisterTopic(topic);
    }

    @Override
    public void close() throws Exception {
        for (AdminClient adminClient : adminClientMap.values()) {
            adminClient.close();
        }
    }

    class AdminClient extends AbstractApiClient implements AutoCloseable {
        public AdminClient(String host, int port) {
            super(host, port);
        }

        @Override
        public void close() throws Exception {
            shutdown();
        }
    }
}
