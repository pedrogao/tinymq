package github.io.pedrogao.mq.broker;

import java.util.List;

public class TopicInfo {
    private String id;

    private List<String> queueIds;

    private int numPartitions;

    public TopicInfo() {
    }

    public TopicInfo(String id, List<String> queueIds, int numPartitions) {
        this.id = id;
        this.queueIds = queueIds;
        this.numPartitions = numPartitions;
    }

    public List<String> getQueueIds() {
        return queueIds;
    }

    public void setQueueIds(List<String> queueIds) {
        this.queueIds = queueIds;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    @Override
    public String toString() {
        return "TopicInfo{" +
                "id='" + id + '\'' +
                ", queueIds=" + queueIds +
                ", numPartitions=" + numPartitions +
                '}';
    }
}
