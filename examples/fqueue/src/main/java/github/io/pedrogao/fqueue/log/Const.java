package github.io.pedrogao.fqueue.log;

public interface Const {
    /**
     * Entity
     */
    String MAGIC = "fqueue";

    String DB_FILE_PREFIX = "fq";

    String DB_FILE_SUFFIX = ".db";


    byte WRITE_SUCCESS = 1;

    byte WRITE_FAILURE = 2;

    byte WRITE_FULL = 3;

    /**
     * Index
     */
    int INDEX_LIMIT_LENGTH = 50;

    String INDEX_FILE_NAME = "fq.index";

    int HEAP_HEAD_LENGTH = 26;
}
