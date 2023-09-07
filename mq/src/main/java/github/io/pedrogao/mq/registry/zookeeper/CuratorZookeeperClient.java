package github.io.pedrogao.mq.registry.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class CuratorZookeeperClient extends AbstractZookeeperClient {
    private final Logger log = LoggerFactory.getLogger(CuratorZookeeperClient.class);

    private CuratorFramework client;

    public CuratorZookeeperClient(String zkAddress) {
        super(zkAddress);

        initClient(zkAddress);
    }

    public CuratorZookeeperClient(String zkAddress, Integer baseSleepTimes, Integer maxRetryTimes) {
        super(zkAddress, baseSleepTimes, maxRetryTimes);

        initClient(zkAddress);
    }

    private void initClient(String zkAddress) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(super.getBaseSleepTimes(), super.getMaxRetryTimes());
        client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();
    }

    @Override
    public Object getClient() {
        return client;
    }

    @Override
    public void createPersistentData(String path, String data) {
        try {
            client.create().creatingParentContainersIfNeeded().
                    withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            log.info("create {} but node exists {}", path, e.getMessage());
        } catch (Exception e) {
            log.error("create persistent node error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createPersistentWithSeqData(String path, String data) {
        try {
            client.create().creatingParentContainersIfNeeded().
                    withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            log.info("create {} but node exists {}", path, e.getMessage());
        } catch (Exception e) {
            log.error("create persistent node with seq error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateNodeData(String path, String data) {
        try {
            client.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            log.error("update node data error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getNodeData(String path) {
        try {
            byte[] bytes = client.getData().forPath(path);
            return new String(bytes);
        } catch (KeeperException.NoNodeException e) {
            // log.error("node not exist", e);
            return null;
        } catch (Exception e) {
            log.error("get node data error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getChildrenData(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            log.error("node not exist", e);
            return Collections.emptyList();
        } catch (Exception e) {
            log.error("get children node data error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTemporarySeqData(String path, String data) {
        try {
            client.create().creatingParentContainersIfNeeded().
                    withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            log.info("create {} but node exists {}", path, e.getMessage());
        } catch (Exception e) {
            log.error("create temporary node with seq error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTemporaryData(String path, String data) {
        try {
            client.create().creatingParentContainersIfNeeded().
                    withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes());
        } catch (KeeperException.NoChildrenForEphemeralsException e) {
            try {
                client.setData().forPath(path, data.getBytes());
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("create {} but node exists {}", path, e.getMessage());
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @Override
    public void updateTemporaryData(String path, String data) {
        try {
            client.setData().forPath(path, data.getBytes());
        } catch (Exception ex) {
            log.error("set temporary node data error", ex);
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() {
        client.close();
    }

    @Override
    public boolean deleteNode(String path) {
        try {
            client.delete().forPath(path);
            return true;
        } catch (KeeperException.NoNodeException e) {
            log.error("node not exist", e);
            return false;
        } catch (Exception e) {
            log.error("delete node error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean existNode(String path) {
        try {
            Stat stat = client.checkExists().forPath(path);
            return stat != null;
        } catch (Exception e) {
            log.error("check node exist error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void watchNodeData(String path, Watcher watcher) {
        try {
            client.getData().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            log.error("watch node data error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void watchChildNodeData(String path, Watcher watcher) {
        try {
            client.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            log.error("watch child node data error", e);
            throw new RuntimeException(e);
        }
    }
}
