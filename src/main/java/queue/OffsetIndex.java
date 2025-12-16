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
    private final List<Long> logicalOffsets = new ArrayList<>();
    private final List<Long> physicalOffsets = new ArrayList<>();
    private final ByteBuffer readBuffer  = ByteBuffer.allocate(ENTRY_SIZE);
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(ENTRY_SIZE);

    public OffsetIndex(Path indexFile) throws IOException {
        this.channel = FileChannel.open(indexFile, CREATE, READ, WRITE);
        loadExistingEntries();
    }

    // -------- Startup loading --------
    private void loadExistingEntries() throws IOException {
        long size = channel.size();

        // Truncate partial entry (crash safety)
        if (size % ENTRY_SIZE != 0) {
            channel.truncate(size - (size % ENTRY_SIZE));
        }

        channel.position(0);

        while (channel.position() < channel.size()) {
            readBuffer.clear();
            readFully(readBuffer);
            readBuffer.flip();

            long logical  = readBuffer.getLong();
            long physical = readBuffer.getLong();

            // Monotonicity check (defensive)
            if (!logicalOffsets.isEmpty()) {
                long lastLogical = logicalOffsets.get(logicalOffsets.size() - 1);
                if (logical <= lastLogical) {
                    throw new IOException(
                            "Corrupt index: logical offsets not strictly increasing");
                }
            }

            logicalOffsets.add(logical);
            physicalOffsets.add(physical);
        }
    }

    // -------- Append new index entry --------
    public void append(long logicalOffset, long physicalOffset) throws IOException {

        // Enforce monotonicity
        if (!logicalOffsets.isEmpty()) {
            long last = logicalOffsets.get(logicalOffsets.size() - 1);
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

        logicalOffsets.add(logicalOffset);
        physicalOffsets.add(physicalOffset);
    }

    // -------- Lookup: floor(logicalOffset) --------
    public long lookupFloor(long targetLogicalOffset) {
        int low = 0;
        int high = logicalOffsets.size() - 1;

        long resultPhysical = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midLogical = logicalOffsets.get(mid);

            if (midLogical <= targetLogicalOffset) {
                resultPhysical = physicalOffsets.get(mid);
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return resultPhysical;
    }

    public long lastIndexedLogicalOffset() {
        if (logicalOffsets.isEmpty()) return -1;
        return logicalOffsets.get(logicalOffsets.size() - 1);
    }
    public int size() {
        return logicalOffsets.size();
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

