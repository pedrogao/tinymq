package github.io.pedrogao.mq.broker;

public class QueueInfo {
    private String id;

    private String brokerId;

    // private long index;
    // private String consumerId;

    public QueueInfo() {
    }

    public QueueInfo(String id, String brokerId) {
        this.id = id;
        this.brokerId = brokerId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }
}
