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

    private ThreadLocal<ByteBuffer> localBuffer;
    private ThreadLocal<ByteBuffer> readBuffer;

    // offset index
    private final OffsetIndex offsetIndex;
    private long nextLogicalOffset;


    public DiskQueue(Path filePath,Path indexPath, boolean durable, int maxMessageSize, int batchLimit) throws IOException {
        this.writeChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.readChannel = FileChannel.open(filePath,
                StandardOpenOption.READ);

        this.durable = durable;
        this.maxMessageSize = maxMessageSize;
        this.batchLimit = batchLimit;
        this.batchCounter = 0;

        // Offset Index
        this.offsetIndex = new OffsetIndex(indexPath);
        long last = offsetIndex.lastIndexedLogicalOffset();
        this.nextLogicalOffset = (last == -1) ? 0 : last + 1;

        this.localBuffer = ThreadLocal.withInitial(
                () -> ByteBuffer.allocateDirect(maxMessageSize + 4)
        );

        this.readBuffer = ThreadLocal.withInitial(
                () -> ByteBuffer.allocateDirect(maxMessageSize)
        );
    }


    public DiskQueue(Path filePath, Path indexPath, boolean durable) throws IOException {
        this(filePath, indexPath, durable, MAX_MESSAGE_SIZE, BATCH_LIMIT);
    }







    public synchronized void enqueue(byte[] data) throws IOException {
        int length = data.length;

        if (length > maxMessageSize)
            throw new IOException("Message too large: " + length);

        // 🔹 NEW: capture physical offset BEFORE write
        long physicalOffset = writeChannel.position();

        ByteBuffer buffer = localBuffer.get();
        buffer.clear();              // reset for this write
        buffer.putInt(length);       // write header
        buffer.put(data);            // write payload
        buffer.flip();               // switch to reading mode for FileChannel.write

        while (buffer.hasRemaining()) {
            writeChannel.write(buffer);
        }

        // 🔹 NEW: logical offset + index append
        long logicalOffset = nextLogicalOffset++;
        offsetIndex.append(logicalOffset, physicalOffset);

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
        long start = readChannel.position();
        long fileSize = writeChannel.size();

        // --- PARTIAL TAIL PROTECTION ---
        if (start + 4 > fileSize) return null; // Not enough bytes for header

        // Reusable read buffer
        ByteBuffer buffer = readBuffer.get();

        // --- READ LENGTH (4 bytes) ---
        buffer.clear();
        buffer.limit(4);

        while (buffer.hasRemaining()) {
            int r = readChannel.read(buffer);
            if (r == -1) return null; // EOF (no full header)
        }

        buffer.flip();
        int length = buffer.getInt();

        // Validate header
        if (length < 0 || length > maxMessageSize)
            throw new IOException("Corrupt length: " + length);

        // --- PARTIAL TAIL PROTECTION #2 ---
        if (start + 4 + length > fileSize)
            return null; // Partial record — writer hasn't finished

        // --- READ PAYLOAD ---
        buffer.clear();
        buffer.limit(length);

        while (buffer.hasRemaining()) {
            int r = readChannel.read(buffer);
            if (r == -1) throw new IOException("Unexpected EOF while reading payload");
        }

        buffer.flip();
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
