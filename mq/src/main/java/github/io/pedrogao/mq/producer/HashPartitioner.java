package github.io.pedrogao.mq.producer;

public class HashPartitioner implements IPartitioner {
    @Override
    public int partition(String key, int numBrokers) {
        int hashCode = key.hashCode();
        return hashCode % numBrokers;
    }
}
