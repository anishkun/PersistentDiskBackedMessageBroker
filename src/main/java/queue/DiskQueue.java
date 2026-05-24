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

    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;
    
    private final Object writeLock = new Object();
    private final Object readLock = new Object();

    // offset index
    private final OffsetIndex offsetIndex;
    private long nextLogicalOffset;
    
    // consumer state
    private final Path consumerIndexPath;
    private long nextReadLogicalOffset;


    public DiskQueue(Path filePath, Path indexPath, Path consumerIndexPath, boolean durable, int maxMessageSize, int batchLimit) throws IOException {
        this.writeChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.readChannel = FileChannel.open(filePath,
                StandardOpenOption.READ);

        this.durable = durable;
        this.maxMessageSize = maxMessageSize;
        this.batchLimit = batchLimit;
        this.batchCounter = 0;
        this.consumerIndexPath = consumerIndexPath;

        // Offset Index
        this.offsetIndex = new OffsetIndex(indexPath);
        long last = offsetIndex.lastIndexedLogicalOffset();
        this.nextLogicalOffset = (last == -1) ? 0 : last + 1;

        this.writeBuffer = ByteBuffer.allocateDirect(maxMessageSize + 4);
        this.readBuffer = ByteBuffer.allocateDirect(maxMessageSize);

        // Load Consumer Offset
        if (java.nio.file.Files.exists(consumerIndexPath)) {
            byte[] data = java.nio.file.Files.readAllBytes(consumerIndexPath);
            if (data.length >= 8) {
                this.nextReadLogicalOffset = ByteBuffer.wrap(data).getLong();
            } else {
                this.nextReadLogicalOffset = 0;
            }
        } else {
            this.nextReadLogicalOffset = 0;
        }

        // Seek readChannel to consumer offset
        long physicalSeek = offsetIndex.lookupFloor(this.nextReadLogicalOffset);
        if (physicalSeek != -1) {
            this.readChannel.position(physicalSeek);
        } else {
            this.readChannel.position(0);
        }
    }

    public DiskQueue(Path filePath, Path indexPath, boolean durable, int maxMessageSize, int batchLimit) throws IOException {
        this(filePath, indexPath, indexPath.resolveSibling(indexPath.getFileName().toString() + ".consumer"), durable, maxMessageSize, batchLimit);
    }

    public DiskQueue(Path filePath, Path indexPath, boolean durable) throws IOException {
        this(filePath, indexPath, durable, MAX_MESSAGE_SIZE, BATCH_LIMIT);
    }







    public void enqueue(byte[] data) throws IOException {
        int length = data.length;

        if (length > maxMessageSize)
            throw new IOException("Message too large: " + length);

        synchronized (writeLock) {
            // 🔹 NEW: capture physical offset BEFORE write
            long physicalOffset = writeChannel.position();

            ByteBuffer buffer = this.writeBuffer;
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
    }

    // OPTIONAL: convenience String version
    public void enqueue(String message) throws IOException {
        enqueue(message.getBytes(StandardCharsets.UTF_8));
    }


    public byte[] dequeue() throws IOException {
        synchronized (readLock) {
            long start = readChannel.position();
            long fileSize = writeChannel.size();

            // --- PARTIAL TAIL PROTECTION ---
            if (start + 4 > fileSize) {
                readChannel.position(start);
                return null; // Not enough bytes for header
            }

            // Reusable read buffer
            ByteBuffer buffer = this.readBuffer;

            // --- READ LENGTH (4 bytes) ---
            buffer.clear();
            buffer.limit(4);

            while (buffer.hasRemaining()) {
                int r = readChannel.read(buffer);
                if (r == -1) {
                    readChannel.position(start);
                    return null; // EOF (no full header)
                }
            }

            buffer.flip();
            int length = buffer.getInt();

            // Validate header
            if (length < 0 || length > maxMessageSize)
                throw new IOException("Corrupt length: " + length);

            // --- PARTIAL TAIL PROTECTION #2 ---
            if (start + 4 + length > fileSize) {
                readChannel.position(start);
                return null; // Partial record — writer hasn't finished
            }

            // --- READ PAYLOAD ---
            buffer.clear();
            buffer.limit(length);

            while (buffer.hasRemaining()) {
                int r = readChannel.read(buffer);
                if (r == -1) {
                    readChannel.position(start);
                    throw new IOException("Unexpected EOF while reading payload");
                }
            }

            buffer.flip();
            byte[] data = new byte[length];
            buffer.get(data);
            
            // Advance consumer offset
            nextReadLogicalOffset++;
            
            return data;
        }
    }



    /** Optional manual flush for durability */
    public void flush() throws IOException {
        synchronized (writeLock) {
            if (durable && batchCounter > 0) {
                writeChannel.force(false);
                batchCounter = 0;
            }
        }
        
        synchronized (readLock) {
            persistConsumerState();
        }
    }

    private void persistConsumerState() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(nextReadLogicalOffset);
        java.nio.file.Files.write(consumerIndexPath, buf.array(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void close() throws IOException {
        if (durable) flush();
        synchronized (writeLock) {
            writeChannel.close();
        }
        synchronized (readLock) {
            persistConsumerState();
            readChannel.close();
        }
    }
}
