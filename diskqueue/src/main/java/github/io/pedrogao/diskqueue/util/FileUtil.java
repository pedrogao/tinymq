package github.io.pedrogao.diskqueue.util;

import java.io.File;
import java.io.IOException;
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

    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;

        File[] subs = dir.listFiles();
        if (subs != null) {
            for (var f : Objects.requireNonNull(dir.listFiles())) {
                if (f.isFile()) {
                    if (!f.delete()) {
                        throw new IllegalStateException("delete file failed: " + f);
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if (!dir.delete()) {
            throw new IllegalStateException("delete directory failed: " + dir);
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists() || !file.isFile())
            return;
        if (!file.delete()) {
            throw new IllegalStateException("delete file failed: " + file);
        }
    }
}
