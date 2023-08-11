package github.io.pedrogao.dqv1;

import java.util.ArrayDeque;
import java.util.Queue;

public class App {
    public static void main(String[] args) {
        Queue<String> queue = new ArrayDeque<>();
        queue.offer("1");
        queue.offer("2");

        while (!queue.isEmpty()) {
            System.out.println(queue.poll());
        }
    }
}
