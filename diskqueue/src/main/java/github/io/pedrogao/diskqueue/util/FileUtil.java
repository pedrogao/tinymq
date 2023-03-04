package github.io.pedrogao.diskqueue.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class FileUtil {

    public static boolean isFileNameValid(String filename) {
        File file = new File(filename);
        try {
            file.getCanonicalPath();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void deleteDirectory(File dir) throws IOException {
        if (!dir.exists()) return;

        File[] subs = dir.listFiles();
        if (subs != null) {
            for (var f : Objects.requireNonNull(dir.listFiles())) {
                if (f.isFile()) {
                    Files.delete(Path.of(f.getPath()));
                } else {
                    deleteDirectory(f);
                }
            }
        }
        Files.delete(Path.of(dir.getPath()));
    }

    public static void deleteFile(File file) throws IOException {
        if (!file.exists() || !file.isFile())
            return;
        Files.delete(Path.of(file.getPath()));
    }
}
