package github.io.pedrogao.mq.consumer;

import github.io.pedrogao.mq.message.Message;

import java.util.stream.Stream;

public class MessageStream {
    private final IFetcher fetcher;

    public MessageStream(String host, int port, String queue, String channel, int batchSize) {
        fetcher = new DefaultFetcher(host, port, queue, channel, batchSize);
    }

    public Stream<Message> getMessageStream() {
        return fetcher.fetch().stream();
    }
}
