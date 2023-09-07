package github.io.pedrogao.mq.consumer;

import github.io.pedrogao.mq.message.Message;

import java.util.List;

public interface IFetcher {
    List<Message> fetch();
}
