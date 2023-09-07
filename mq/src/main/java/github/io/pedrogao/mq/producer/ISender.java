package github.io.pedrogao.mq.producer;

public interface ISender {
    void send(byte[] payload);

    void send(String payload);
}
