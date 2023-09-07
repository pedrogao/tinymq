package github.io.pedrogao.mq.console;

import github.io.pedrogao.mq.consumer.Consumer;
import github.io.pedrogao.mq.consumer.ConsumerConfig;
import github.io.pedrogao.mq.consumer.MessageStream;

public class ConsumerConsole {
    public static void main(String[] args) {
        final String zkAddress = args.length > 0 && args[0] != null ? args[0] : "127.0.0.1:2181";
        final int batchSize = args.length > 1 && args[1] != null ? Integer.parseInt(args[1]) : 10;
        final String topic = args.length > 2 && args[2] != null ? args[2] : "test";

        // Consumer
        final ConsumerConfig config = new ConsumerConfig(zkAddress, batchSize);
        final Consumer consumer = new Consumer(config);

        // Message stream
        final MessageStream messageStream = consumer.getMessageStream(topic, "test-channel");

        // Handle messages
        messageStream.getMessageStream().forEach(message -> {
            System.out.println("Message: " + message);
        });
    }
}
