package queue;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;

public final class OffsetIndex {

    private static final int ENTRY_SIZE = 16; // 8 bytes logical + 8 bytes physical

    private final FileChannel channel;

    // In-memory sparse index (monotonic)
    private long[] logicalOffsets = new long[1024];
    private long[] physicalOffsets = new long[1024];
    private int size = 0;
    private final ByteBuffer readBuffer  = ByteBuffer.allocate(ENTRY_SIZE);
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(ENTRY_SIZE);

    private void ensureCapacity() {
        if (size == logicalOffsets.length) {
            int newCapacity = logicalOffsets.length * 2;
            logicalOffsets = java.util.Arrays.copyOf(logicalOffsets, newCapacity);
            physicalOffsets = java.util.Arrays.copyOf(physicalOffsets, newCapacity);
        }
    }

    public OffsetIndex(Path indexFile) throws IOException {
        this.channel = FileChannel.open(indexFile, CREATE, READ, WRITE);
        loadExistingEntries();
    }

    // -------- Startup loading --------
    private void loadExistingEntries() throws IOException {
        long fileSize = channel.size();

        // Truncate partial entry (crash safety)
        if (fileSize % ENTRY_SIZE != 0) {
            channel.truncate(fileSize - (fileSize % ENTRY_SIZE));
        }

        channel.position(0);

        while (channel.position() < channel.size()) {
            readBuffer.clear();
            readFully(readBuffer);
            readBuffer.flip();

            long logical  = readBuffer.getLong();
            long physical = readBuffer.getLong();

            // Monotonicity check (defensive)
            if (size > 0) {
                long lastLogical = logicalOffsets[size - 1];
                if (logical <= lastLogical) {
                    throw new IOException(
                            "Corrupt index: logical offsets not strictly increasing");
                }
            }

            ensureCapacity();
            logicalOffsets[size] = logical;
            physicalOffsets[size] = physical;
            size++;
        }
    }

    // -------- Append new index entry --------
    public void append(long logicalOffset, long physicalOffset) throws IOException {

        // Enforce monotonicity
        if (size > 0) {
            long last = logicalOffsets[size - 1];
            if (logicalOffset <= last) {
                throw new IllegalArgumentException(
                        "Logical offsets must be strictly increasing");
            }
        }

        writeBuffer.clear();
        writeBuffer.putLong(logicalOffset);
        writeBuffer.putLong(physicalOffset);
        writeBuffer.flip();

        while (writeBuffer.hasRemaining()) {
            channel.write(writeBuffer);
        }

        ensureCapacity();
        logicalOffsets[size] = logicalOffset;
        physicalOffsets[size] = physicalOffset;
        size++;
    }

    // -------- Lookup: floor(logicalOffset) --------
    public long lookupFloor(long targetLogicalOffset) {
        int low = 0;
        int high = size - 1;

        long resultPhysical = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midLogical = logicalOffsets[mid];

            if (midLogical <= targetLogicalOffset) {
                resultPhysical = physicalOffsets[mid];
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return resultPhysical;
    }

    public long lastIndexedLogicalOffset() {
        if (size == 0) return -1;
        return logicalOffsets[size - 1];
    }
    public int size() {
        return size;
    }

    public void flush() throws IOException {
        channel.force(false);
    }

    public void close() throws IOException {
        channel.close();
    }

    // -------- helpers --------
    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int r = channel.read(buf);
            if (r < 0)
                throw new IOException("Unexpected EOF while reading index");
        }
    }
}

