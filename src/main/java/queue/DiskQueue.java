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

    private final FileChannel writeChannel;
    private final FileChannel readChannel;
    private final boolean durable;
    private int batchCounter = 0;

    public DiskQueue(Path filePath, boolean durable) throws IOException {
        this.writeChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.readChannel = FileChannel.open(filePath,
                StandardOpenOption.READ);
        this.durable = durable;
    }

    private final ThreadLocal<ByteBuffer> localBuffer = ThreadLocal.withInitial(
            () -> ByteBuffer.allocateDirect(MAX_MESSAGE_SIZE + 4)
    );

    public synchronized void enqueue(byte[] data) throws IOException {
        int length = data.length;

        if (length > MAX_MESSAGE_SIZE)
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
            if (batchCounter >= BATCH_LIMIT) {
                writeChannel.force(false);
                batchCounter = 0;
            }
        }
    }

    // OPTIONAL: convenience String version
    public void enqueue(String message) throws IOException {
        enqueue(message.getBytes(StandardCharsets.UTF_8));
    }


    public synchronized String dequeue() throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        while (lenBuf.hasRemaining()) {
            int r = readChannel.read(lenBuf);
            if (r == -1) return null; // EOF
        }

        lenBuf.flip();
        int length = lenBuf.getInt();
        if (length < 0 || length > MAX_MESSAGE_SIZE)
            throw new IOException("Corrupt length: " + length);

        ByteBuffer msgBuf = ByteBuffer.allocate(length);
        while (msgBuf.hasRemaining()) {
            int r = readChannel.read(msgBuf);
            if (r == -1) throw new IOException("Unexpected EOF in message");
        }

        msgBuf.flip();
        byte[] data = new byte[length];
        msgBuf.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Optional manual flush for durability */
    public synchronized void flush() throws IOException {
        if (durable && batchCounter > 0) {
            writeChannel.force(false);
            batchCounter = 0;
        }
    }

    public void close() throws IOException {
        flush();
        writeChannel.close();
        readChannel.close();
    }
}
