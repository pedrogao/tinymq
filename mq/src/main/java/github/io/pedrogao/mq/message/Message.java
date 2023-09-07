package github.io.pedrogao.mq.message;

import java.nio.ByteBuffer;

// Message format: copy from nsq
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//	|       8-byte         ||    ||                 16-byte                      || N-byte
//	------------------------------------------------------------------------------------------...
//	  nanosecond timestamp    ^^                   message ID                       message body
//	                       (uint16)
//	                        2-byte
//	                       attempts
//
public class Message {
    public static final int MsgIdLength = 16;
    public static final int MsgHeaderSize = MsgIdLength + 8 + 2; // Timestamp + Attempts

    private byte[] id;
    private long timestamp;
    private short attempts;
    private byte[] body;

    public Message(byte[] id, byte[] body) {
        this.id = id;
        this.body = body;
        this.attempts = 0;
        this.timestamp = System.currentTimeMillis();
    }

    public byte[] getId() {
        return id;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public short getAttempts() {
        return attempts;
    }

    public void setAttempts(short attempts) {
        this.attempts = attempts;
    }

    public void incrAttempts() {
        this.attempts++;
    }

    public byte[] getBody() {
        return body;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(MsgHeaderSize + body.length);
        buffer.put(id);
        buffer.putLong(timestamp);
        buffer.putShort(attempts);
        buffer.put(body);
        return buffer.array();
    }

    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] id = new byte[MsgIdLength];
        buffer.get(id);
        long timestamp = buffer.getLong();
        short attempts = buffer.getShort();
        byte[] body = new byte[data.length - MsgHeaderSize];
        buffer.get(body);

        Message msg = new Message(id, body);
        msg.setTimestamp(timestamp);
        msg.setAttempts(attempts);
        return msg;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + new String(id) +
                ", timestamp=" + timestamp +
                ", attempts=" + attempts +
                ", body=" + new String(body) +
                '}';
    }
}
