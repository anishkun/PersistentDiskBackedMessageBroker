package app;

import queue.DiskQueue;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        try {
            // Create queue with durability enabled (true)
            DiskQueue queue = new DiskQueue(Paths.get("queue.log"), true);

            // Writing a few messages
            queue.enqueue("Hello, World!");
            queue.enqueue("This is a persistent message queue.");
            queue.enqueue("Messages are stored on disk sequentially.");
            System.out.println("Messages written to disk.");

            // Manually flush to ensure last batch persisted
            queue.flush();

            // Close writer
            queue.close();

            // Reopen queue for reading
            queue = new DiskQueue(Paths.get("queue.log"), true);

            // Reading messages back
            String msg;
            while ((msg = queue.dequeue()) != null) {
                System.out.println("Read from disk: " + msg);
            }

            queue.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
