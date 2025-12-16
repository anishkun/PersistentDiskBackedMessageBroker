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

    public OffsetIndex(Path indexFile) throws IOException {
        this.channel = FileChannel.open(indexFile, CREATE, READ, WRITE);
        loadExistingEntries();
    }

    // -------- Startup loading --------
    private void loadExistingEntries() throws IOException {
        long size = channel.size();
        if (size % ENTRY_SIZE != 0) {
            // Truncate partial entry (crash safety)
            channel.truncate(size - (size % ENTRY_SIZE));
        }

        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        channel.position(0);

        while (channel.position() < channel.size()) {
            buf.clear();
            readFully(buf);
            buf.flip();

            long logical = buf.getLong();
            long physical = buf.getLong();

            logicalOffsets.add(logical);
            physicalOffsets.add(physical);
        }
    }

    // -------- Append new index entry --------
    public void append(long logicalOffset, long physicalOffset) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putLong(logicalOffset);
        buf.putLong(physicalOffset);
        buf.flip();

        while (buf.hasRemaining()) {
            channel.write(buf);
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

