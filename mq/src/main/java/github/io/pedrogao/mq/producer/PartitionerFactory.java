package github.io.pedrogao.mq.producer;

public class PartitionerFactory {
    public static IPartitioner getPartitioner(PartitionType type) {
        return switch (type) {
            case RANDOM -> new RandomPartitioner();
            case HASH -> new HashPartitioner();
            default -> throw new IllegalArgumentException("Invalid partitioner type: " + type);
        };
    }
}
