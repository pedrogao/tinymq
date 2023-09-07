package github.io.pedrogao.mq.console;

import github.io.pedrogao.mq.admin.AdminManager;

public class AdminConsole {

    public static void main(String[] args) throws Exception {
        final String zkAddress = args.length > 0 && args[0] != null ? args[0] : "127.0.0.1:2181";
        final String operationType = args.length > 1 && args[1] != null ? args[1] : "createTopic";
        final String topic = args.length > 2 && args[2] != null ? args[2] : "test";
        final int partitionNum = args.length > 3 && args[3] != null ? Integer.parseInt(args[3]) : 1;

        try (AdminManager adminManager = new AdminManager(zkAddress)) {
            switch (operationType) {
                case "createTopic" -> adminManager.createTopic(topic, partitionNum);
                case "deleteTopic" -> adminManager.deleteTopic(topic);
                case "none" -> System.out.println("No operation type specified");
            }
        }
    }
}
