package github.io.pedrogao.mq.console;

import github.io.pedrogao.mq.producer.*;

public class ProducerConsole {

    public static void main(String[] args) {
        final String zkAddress = args.length > 0 && args[0] != null ? args[0] : "127.0.0.1:2181";
        final String partitionType = args.length > 1 && args[1] != null ? args[1] : "hash";
        final String topic = args.length > 2 && args[2] != null ? args[2] : "test";

        final ProducerConfig config = new ProducerConfig(zkAddress, PartitionType.fromString(partitionType));
        final Producer producer = new Producer(config);

        // Get sender pool
        SenderPool senderPool = producer.getSenderPool(topic);
        for (int i = 0; i < 100; i++) {
            senderPool.send("hello world!");
        }
    }
}
