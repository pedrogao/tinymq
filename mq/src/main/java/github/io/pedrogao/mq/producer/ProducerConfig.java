package github.io.pedrogao.mq.producer;

public class ProducerConfig {

    private String zkAddress;

    private PartitionType partitionType;

    public ProducerConfig(String zkAddress, PartitionType partitionType) {
        this.zkAddress = zkAddress;
        this.partitionType = partitionType;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }
}
