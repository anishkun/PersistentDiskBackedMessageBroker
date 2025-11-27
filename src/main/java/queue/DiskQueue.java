package queue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DiskQueue {
    private static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10 MB
    private static final int BATCH_LIMIT = 100; // flush every 100 messages
    private final int maxMessageSize;
    private final int batchLimit;


    private final FileChannel writeChannel;
    private final FileChannel readChannel;
    private final boolean durable;
    private int batchCounter = 0;

    public DiskQueue(Path filePath, boolean durable, int maxMessageSize, int batchLimit) throws IOException {
        this.writeChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.readChannel = FileChannel.open(filePath,
                StandardOpenOption.READ);

        this.durable = durable;
        this.maxMessageSize = maxMessageSize;
        this.batchLimit = batchLimit;
        this.batchCounter = 0;

        this.localBuffer = ThreadLocal.withInitial(
                () -> ByteBuffer.allocateDirect(maxMessageSize + 4)
        );

        this.readBuffer = ThreadLocal.withInitial(
                () -> ByteBuffer.allocateDirect(maxMessageSize)
        );
    }


    public DiskQueue(Path filePath, boolean durable) throws IOException {
        this(filePath, durable, MAX_MESSAGE_SIZE, BATCH_LIMIT);
    }


    private ThreadLocal<ByteBuffer> localBuffer;
    private ThreadLocal<ByteBuffer> readBuffer;



    public synchronized void enqueue(byte[] data) throws IOException {
        int length = data.length;

        if (length > maxMessageSize)
            throw new IOException("Message too large: " + length);

        ByteBuffer buffer = localBuffer.get();
        buffer.clear();              // reset for this write
        buffer.putInt(length);       // write header
        buffer.put(data);            // write payload
        buffer.flip();               // switch to reading mode for FileChannel.write

        while (buffer.hasRemaining()) {
            writeChannel.write(buffer);
        }

        if (durable) {
            batchCounter++;
            if (batchCounter >= batchLimit) {
                writeChannel.force(false);
                batchCounter = 0;
            }
        }
    }

    // OPTIONAL: convenience String version
    public void enqueue(String message) throws IOException {
        enqueue(message.getBytes(StandardCharsets.UTF_8));
    }


    public synchronized byte[] dequeue() throws IOException {
        // Reusable read buffer
        ByteBuffer buffer = readBuffer.get();

        // --- READ LENGTH (4 bytes) ---
        buffer.clear();
        buffer.limit(4);

        while (buffer.hasRemaining()) {
            int r = readChannel.read(buffer);
            if (r == -1) return null; // EOF
        }

        buffer.flip();
        int length = buffer.getInt();

        if (length < 0 || length > maxMessageSize) {
            throw new IOException("Corrupt length: " + length);
        }

        // --- READ PAYLOAD ---
        buffer.clear();
        buffer.limit(length);

        while (buffer.hasRemaining()) {
            int r = readChannel.read(buffer);
            if (r == -1) throw new IOException("Unexpected EOF while reading message");
        }

        buffer.flip();

        // Extract data into a byte[] (needed for String)
        byte[] data = new byte[length];
        buffer.get(data);

        return data;
    }


    /** Optional manual flush for durability */
    public synchronized void flush() throws IOException {
        if (durable && batchCounter > 0) {
            writeChannel.force(false);
            batchCounter = 0;
        }
    }

    public void close() throws IOException {
        if (durable) flush();
        writeChannel.close();
        readChannel.close();
    }
}
