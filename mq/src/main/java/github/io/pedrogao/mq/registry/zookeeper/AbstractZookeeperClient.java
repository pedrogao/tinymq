package github.io.pedrogao.mq.registry.zookeeper;

import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.Objects;

public abstract class AbstractZookeeperClient {
    private String zkAddress;
    private int baseSleepTimes;
    private int maxRetryTimes;

    public AbstractZookeeperClient(String zkAddress) {
        this.zkAddress = zkAddress;
        this.baseSleepTimes = 1000;
        this.maxRetryTimes = 3;
    }

    public AbstractZookeeperClient(String zkAddress, Integer baseSleepTimes, Integer maxRetryTimes) {
        this.zkAddress = zkAddress;
        this.baseSleepTimes = Objects.requireNonNullElse(baseSleepTimes, 1000);
        this.maxRetryTimes = Objects.requireNonNullElse(maxRetryTimes, 3);
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public int getBaseSleepTimes() {
        return baseSleepTimes;
    }

    public void setBaseSleepTimes(int baseSleepTimes) {
        this.baseSleepTimes = baseSleepTimes;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public abstract Object getClient();

    public abstract void createPersistentData(String address, String data);

    public abstract void createPersistentWithSeqData(String address, String data);

    public abstract String getNodeData(String path);

    public abstract void updateNodeData(String address, String data);

    public abstract List<String> getChildrenData(String path);

    public abstract void createTemporaryData(String address, String data);

    public abstract void createTemporarySeqData(String address, String data);

    public abstract void updateTemporaryData(String address, String data);

    public abstract boolean deleteNode(String address);

    public abstract boolean existNode(String address);

    public abstract void watchNodeData(String path, Watcher watcher);

    public abstract void watchChildNodeData(String path, Watcher watcher);

    public abstract void destroy();
}
