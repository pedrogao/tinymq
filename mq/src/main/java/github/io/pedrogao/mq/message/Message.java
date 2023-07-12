package github.io.pedrogao.mq.message;

import java.nio.ByteBuffer;

public class Message {
    private static final byte MAGIC_VERSION = 1;

    final ByteBuffer buffer;

    private final int messageSize;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
        this.messageSize = buffer.limit();
    }
}
