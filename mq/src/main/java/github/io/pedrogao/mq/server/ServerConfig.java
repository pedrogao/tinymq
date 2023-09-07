package github.io.pedrogao.mq.server;

public class ServerConfig {
    private int port;

    private String host;

    private String brokerId;

    private String dataDir;

    private String zkAddress;

    private int queueFlushIntervalMs;

    private int queueCleanIntervalMs;

    private int queueExpireIntervalHour;

    private int registerRefreshIntervalMs;


    public ServerConfig(String brokerId, String host, String zkAddress) {
        this.brokerId = brokerId;
        this.zkAddress = zkAddress;
        this.host = host;
        this.port = 7070;
        this.dataDir = "/tmp/tinymq";
        this.queueFlushIntervalMs = 1000;      // 1s
        this.queueCleanIntervalMs = 1000 * 10; // 10s
        this.queueExpireIntervalHour = 24;
        this.registerRefreshIntervalMs = 1000 * 10; // 10s
    }

    public ServerConfig(String brokerId, String host, String zkAddress, String dataDir) {
        this(brokerId, host, zkAddress);
        this.dataDir = dataDir;
    }

    public ServerConfig(String brokerId, String host, String zkAddress, String dataDir, int port) {
        this(brokerId, host, zkAddress);
        this.dataDir = dataDir;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public int getQueueFlushIntervalMs() {
        return queueFlushIntervalMs;
    }

    public void setQueueFlushIntervalMs(int queueFlushIntervalMs) {
        this.queueFlushIntervalMs = queueFlushIntervalMs;
    }

    public int getQueueCleanIntervalMs() {
        return queueCleanIntervalMs;
    }

    public void setQueueCleanIntervalMs(int queueCleanIntervalMs) {
        this.queueCleanIntervalMs = queueCleanIntervalMs;
    }

    public int getQueueExpireIntervalHour() {
        return queueExpireIntervalHour;
    }

    public void setQueueExpireIntervalHour(int queueExpireIntervalHour) {
        this.queueExpireIntervalHour = queueExpireIntervalHour;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public int getRegisterRefreshIntervalMs() {
        return registerRefreshIntervalMs;
    }

    public void setRegisterRefreshIntervalMs(int registerRefreshIntervalMs) {
        this.registerRefreshIntervalMs = registerRefreshIntervalMs;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
