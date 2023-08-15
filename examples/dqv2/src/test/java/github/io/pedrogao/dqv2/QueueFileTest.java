package github.io.pedrogao.dqv2;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class QueueFileTest {

    @Test
    void readOverSize() {
    }

    @Test
    void writeOverSize() throws IOException {
        // File file = Files.createTempDirectory("queue").toFile();
        File file = new File("C:\\Users\\Administrator\\Desktop\\projects\\tinymq\\data");
        QueueFile queueFile = new QueueFile(file, 100, true);

        for (int i = 0; i < 100; i++) {
            queueFile.write(("Hello Queue" + i).getBytes());
        }
        long size = queueFile.size();
        assertEquals(size, 100);

        for (int i = 0; i < 100; i++) {
            byte[] bytes = queueFile.read();
            assertEquals(bytes.length, ("Hello Queue" + i).getBytes().length, "not equals at " + i);
        }
    }
}