package github.io.pedrogao.diskqueue.page;

import java.io.IOException;
import java.util.Set;

public interface IMappedPageFactory {

    IMappedPage acquirePage(long index) throws IOException;

    void releasePage(long index);

    int getPageSize();

    String getPageDir();

    void deletePage(long index) throws IOException;

    void deletePages(Set<Long> indexes) throws IOException;

    void deleteAllPages() throws IOException;

    void releaseCachedPages() throws IOException;

    Set<Long> getPageIndexSetBefore(long timestamp);

    void deletePagesBefore(long timestamp) throws IOException;

    void deletePagesBeforePageIndex(long pageIndex) throws IOException;

    long getPageFileLastModifiedTime(long index);

    long getFirstPageIndexBefore(long timestamp);

    Set<Long> getExistingBackFileIndexSet();

    int getCacheSize();

    void flush();

    Set<String> getBackPageFileSet();

    long getBackPageFileSize();
}
