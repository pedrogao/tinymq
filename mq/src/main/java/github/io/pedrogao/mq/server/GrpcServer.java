package github.io.pedrogao.mq.server;

import github.io.pedrogao.mq.queue.QueueManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GrpcServer {

    private final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private Server server;

    private final QueueManager queueManager;

    public GrpcServer(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    public void start(int port) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(queueManager)
                .build()
                .start();

        log.info("Server started, listening on " + port);
    }

    public void close() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
