package github.io.pedrogao.fqueue.log;

import github.io.pedrogao.fqueue.exception.FileFormatException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexFileTest {

    @Test
    void putWriterPosition() throws FileFormatException, IOException {
        IndexFile indexFile = new IndexFile("C:\\Users\\Administrator\\Desktop\\projects\\tinymq\\data");
        indexFile.putWriterPosition(1);
        assertEquals(indexFile.getWriterPosition(), 1);
    }

    @Test
    void putReaderPosition() {
    }

    @Test
    void putWriterIndex() {
    }

    @Test
    void putReaderIndex() {
    }

    @Test
    void incrementSize() {
    }

    @Test
    void decrementSize() {
    }

    @Test
    void getMagicString() {
    }

    @Test
    void getVersion() {
    }

    @Test
    void getReaderPosition() {
    }

    @Test
    void getWriterPosition() {
    }

    @Test
    void getReaderIndex() {
    }

    @Test
    void getWriterIndex() {
    }

    @Test
    void getSize() {
    }

    @Test
    void clear() {
    }

    @Test
    void close() {
    }
}