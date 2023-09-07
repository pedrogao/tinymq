package github.io.pedrogao.mq.server;

import github.io.pedrogao.mq.broker.BrokerInfo;
import github.io.pedrogao.mq.queue.QueueManager;
import github.io.pedrogao.mq.registry.RegistryService;
import github.io.pedrogao.mq.registry.ZookeeperRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class Tiny {

    private final static Logger log = LoggerFactory.getLogger(Tiny.class);

    private final ScheduledExecutorService registerExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ServerConfig config;

    private final RegistryService registryService;
    private final QueueManager queueManager;
    private final GrpcServer server;
    private final BrokerInfo brokerInfo;


    public Tiny(ServerConfig config) {
        this.config = config;
        this.brokerInfo = new BrokerInfo(config.getBrokerId(), config.getHost(), config.getPort());

        registryService = new ZookeeperRegistry(config.getZkAddress());
        queueManager = new QueueManager(config.getDataDir(),
                registryService,
                config.getQueueFlushIntervalMs(),
                config.getQueueCleanIntervalMs(),
                config.getQueueExpireIntervalHour());
        server = new GrpcServer(queueManager);

        registerExecutor.schedule(this::refreshRegister, config.getRegisterRefreshIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void refreshRegister() {
        registryService.registerBroker(config.getBrokerId(), brokerInfo);
    }

    public void start() throws IOException {
        server.start(config.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            log.info("*** shutting down broker server since JVM is shutting down");
            try {
                close();
            } catch (InterruptedException | IOException e) {
                log.error("broker close err ", e);
            }
            log.info("*** broker server shut down");
        }));
    }

    public void close() throws InterruptedException, IOException {
        server.close();
        queueManager.close();
        registryService.close();
        registerExecutor.close();
    }


    public void blockUntilShutdown() throws InterruptedException {
        server.blockUntilShutdown();
    }

    public static void main(String[] args) throws Exception {
        final String dataDir = args.length > 0 && args[0] != null ? args[0] : "C:\\Users\\Administrator\\Desktop\\projects\\tinymq\\data";
        final int port = args.length > 1 && args[1] != null ? Integer.parseInt(args[1]) : 7070;
        final String brokerId = args.length > 2 && args[2] != null ? args[2] : "Broker" + new Random().nextInt();
        final String zkAddress = args.length > 2 && args[3] != null ? args[3] : "127.0.0.1:2181";

        final ServerConfig serverConfig = new ServerConfig(brokerId, "127.0.0.1", zkAddress, dataDir, port);
        final Tiny app = new Tiny(serverConfig);
        app.start();
        app.blockUntilShutdown();
        app.close();
    }
}
