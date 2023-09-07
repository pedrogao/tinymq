package github.io.pedrogao.mq.producer;

public enum PartitionType {
    RANDOM("random"),
    HASH("hash");

    private final String value;

    PartitionType(final String value) {
        this.value = value;
    }

    public static PartitionType fromString(String value) {
        for (PartitionType type : PartitionType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid partitioner type: " + value);
    }

    @Override
    public String toString() {
        return value;
    }
}
