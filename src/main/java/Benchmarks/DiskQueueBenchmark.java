package Benchmarks;

import java.nio.file.Path;
import java.util.Random;
import queue.DiskQueue;

public class DiskQueueBenchmark {

    private static String randomMessage(int size, Random r) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + r.nextInt(26)));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        Path path = Path.of("benchmark_queue.dat");

        int messages = 10_000;
        int messageSize = 10 * 1024;  // 10 KB messages

        boolean durable = true;
        int batchLimit = 100;
        int maxMessageSize = 10 * 1024 * 1024;

        DiskQueue queue = new DiskQueue(path, durable, maxMessageSize, batchLimit);

        // ----------------------------------------
        // GENERATE ALL MESSAGES BEFORE BENCHMARK
        // ----------------------------------------
        System.out.println("Generating messages...");
        Random r = new Random();
        String[] payloads = new String[messages];

        for (int i = 0; i < messages; i++) {
            payloads[i] = randomMessage(messageSize, r);
        }

        // ----------------------------------------
        // WRITE BENCHMARK (NO RANDOM COST)
        // ----------------------------------------
        long startWrite = System.currentTimeMillis();

        for (int i = 0; i < messages; i++) {
            queue.enqueue(payloads[i]);
        }

        long endWrite = System.currentTimeMillis();

        System.out.println("------ WRITE BENCHMARK ------");
        System.out.println("Messages: " + messages);
        System.out.println("Total Write Time: " + (endWrite - startWrite) + " ms");
        System.out.println("Avg per message: " + ((endWrite - startWrite) / (double) messages) + " ms");
        System.out.println("Write Throughput: " + (messages * 1000L / (endWrite - startWrite)) + " msg/sec");


        // ----------------------------------------
        // READ BENCHMARK
        // ----------------------------------------
        long startRead = System.currentTimeMillis();
        int readCount = 0;

        while (true) {
            String msg = queue.dequeue();
            if (msg == null) break;
            readCount++;
        }

        long endRead = System.currentTimeMillis();

        System.out.println("\n------ READ BENCHMARK ------");
        System.out.println("Messages Read: " + readCount);
        System.out.println("Total Read Time: " + (endRead - startRead) + " ms");
        System.out.println("Avg per message: " + ((endRead - startRead) / (double) readCount) + " ms");
        System.out.println("Read Throughput: " + (readCount * 1000L / (endRead - startRead)) + " msg/sec");


        // ----------------------------------------
        // END TO END
        // ----------------------------------------
        long totalTime = (endWrite - startWrite) + (endRead - startRead);
        System.out.println("\n------ END TO END ------");
        System.out.println("Total Time: " + totalTime + " ms");
        System.out.println("End-to-end Throughput: " + (messages * 1000L / totalTime) + " msg/sec");
    }
}

