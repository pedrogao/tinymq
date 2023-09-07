package github.io.pedrogao.mq.producer;

public interface IPartitioner {
    int partition(String key, int numBrokers);
}
