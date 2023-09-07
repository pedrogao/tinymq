package github.io.pedrogao.mq.producer;

import java.util.Random;

public class RandomPartitioner implements IPartitioner {
    private final Random random = new Random();

    @Override
    public int partition(String key, int numBrokers) {
        return random.nextInt(numBrokers);
    }
}
