package github.io.pedrogao.mq.consumer;

public class ConsumerConfig {
    private int batchSize;

    private String zkAddress;

    public ConsumerConfig(String zkAddress, int batchSize) {
        this.zkAddress = zkAddress;
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }
}
