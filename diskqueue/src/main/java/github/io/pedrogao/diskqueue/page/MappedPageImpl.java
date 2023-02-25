package github.io.pedrogao.diskqueue.page;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedPageImpl implements IMappedPage, Closeable {

    private final static Logger logger = LoggerFactory.getLogger(MappedPageImpl.class);

    private ThreadLocalBuffer threadLocalBuffer;

    private volatile boolean dirty = false;

    private volatile boolean closed = false;

    private final String pageFile;

    private final long index;

    public MappedPageImpl(MappedByteBuffer mappedByteBuffer,
                          String pageFile, long index) {
        this.threadLocalBuffer = new ThreadLocalBuffer(mappedByteBuffer);
        this.pageFile = pageFile;
        this.index = index;
    }

    @Override
    public ByteBuffer getLocal(int position) {
        var buffer = this.threadLocalBuffer.get();
        buffer.position(position);
        return buffer;
    }

    @Override
    public byte[] getLocal(int position, int length) {
        var buffer = this.getLocal(position);
        var data = new byte[length];
        buffer.get(data);
        return data;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public String getPageFile() {
        return pageFile;
    }

    @Override
    public long getPageIndex() {
        return index;
    }

    @Override
    public void flush() {
        synchronized (this) {
            if (closed)
                return;
            if (dirty) {
                var buffer = (MappedByteBuffer) threadLocalBuffer.getBuffer();
                buffer.force(); // flush changes to disk
                dirty = false;
                if (logger.isDebugEnabled()) {
                    logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (closed)
                return;

            flush();

            var buffer = (MappedByteBuffer) threadLocalBuffer.getBuffer();
            unmap(buffer);

            this.threadLocalBuffer = null; // for gc
            closed = true;
            if (logger.isDebugEnabled()) {
                logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
            }
        }
    }

    public String toString() {
        return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
    }

    private static void unmap(MappedByteBuffer buffer) {
        closeDirectBuffer(buffer);
        // Cleaner.clean(buffer);
    }

    private static void closeDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) return;
        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                Method cleaner = buffer.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(buffer));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, buffer);
            }
        } catch (Exception ignored) {
        }
        buffer = null;
    }


    /**
     * clean direct buffers
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
            if (buffer == null)
                return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }

    /**
     * thread local buffer
     */
    private static class ThreadLocalBuffer extends ThreadLocal<ByteBuffer> {
        private final ByteBuffer buffer;

        public ThreadLocalBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        protected ByteBuffer initialValue() {
            return buffer.duplicate();
        }
    }
}
