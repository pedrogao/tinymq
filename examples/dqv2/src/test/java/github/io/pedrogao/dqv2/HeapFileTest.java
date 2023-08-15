package github.io.pedrogao.dqv2;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class HeapFileTest {

    @Test
    void getSize() throws IOException {
        File file = File.createTempFile("dqv1", ".q");
        var heapFile = new HeapFile(file, 100, true);
        assertEquals(heapFile.getSize(), 12);
        assertEquals(heapFile.getFileLength(), 100);

        heapFile.write(12, "Hello Queue".getBytes());
        assertEquals(heapFile.getSize(), 12 + "Hello Queue".getBytes().length + 4);
    }
}