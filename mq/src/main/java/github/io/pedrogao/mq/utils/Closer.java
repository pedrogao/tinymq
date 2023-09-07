package github.io.pedrogao.mq.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Selector;

public final class Closer {

    private static final Logger logger = LoggerFactory.getLogger(Closer.class);

    public static void close(java.io.Closeable closeable) throws IOException {
        close(closeable, logger);
    }

    public static void close(java.io.Closeable closeable, Logger logger) throws IOException {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    public static void closeQuietly(Selector selector) {
        closeQuietly(selector, logger);
    }

    public static void closeQuietly(java.io.Closeable closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(Selector closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(ServerSocket serverSocket) {
        if (serverSocket == null) return;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Close a closeable object quietly(not throwing {@link IOException})
     *
     * @param closeable A closeable object
     * @see java.io.Closeable
     */
    public static void closeQuietly(java.io.Closeable closeable) {
        closeQuietly(closeable, logger);
    }
}
