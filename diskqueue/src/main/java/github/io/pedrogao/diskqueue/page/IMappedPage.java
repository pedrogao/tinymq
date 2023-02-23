package github.io.pedrogao.diskqueue.page;

import java.nio.ByteBuffer;

/**
 * Memory mapped page file
 */
public interface IMappedPage {

    ByteBuffer getLocal(int position);

    byte[] getLocal(int position, int length);

    boolean isClosed();

    void setDirty(boolean dirty);

    String getPageFile();

    long getPageIndex();

    void flush();
}
