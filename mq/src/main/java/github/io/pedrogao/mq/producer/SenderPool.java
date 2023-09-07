package github.io.pedrogao.mq.producer;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.broker.QueueInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SenderPool {
    private final List<BrokerInfo> brokerInfoList;

    private final Map<String, ISender> senderMap;

    private final IPartitioner partitioner;

    private final List<QueueInfo> queueInfoList;

    public SenderPool(List<BrokerInfo> brokerInfoList, List<QueueInfo> queueInfoList, IPartitioner partitioner) {
        this.brokerInfoList = brokerInfoList;
        this.senderMap = new ConcurrentHashMap<>();
        this.partitioner = partitioner;
        this.queueInfoList = queueInfoList;

        buildPool();
    }

    private void buildPool() {
        for (int i = 0; i < brokerInfoList.size(); i++) {
            BrokerInfo brokerInfo = brokerInfoList.get(i);
            QueueInfo queueInfo = queueInfoList.get(i);
            ISender sender = new Sender(brokerInfo.getHost(), brokerInfo.getPort(), queueInfo.getId());
            senderMap.put(brokerInfo.getId(), sender);
        }
    }

    private ISender getSender(String key) {
        int index = partitioner.partition(key, brokerInfoList.size());
        return senderMap.get(brokerInfoList.get(index).getId());
    }

    public void send(byte[] payload) {
        ISender sender = getSender("");
        sender.send(payload);
    }

    public void send(String payload) {
        ISender sender = getSender("");
        sender.send(payload);
    }


    public void send(String key, byte[] payload) {
        ISender sender = getSender(key);
        sender.send(payload);
    }

    public void send(String key, String payload) {
        ISender sender = getSender(key);
        sender.send(payload);
    }

}
