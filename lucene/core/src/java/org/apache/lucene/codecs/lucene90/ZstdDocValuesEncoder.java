package org.apache.lucene.codecs.lucene90;

import com.github.luben.zstd.Zstd;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/** Compressed blocks of doc values. */
public class ZstdDocValuesEncoder implements BaseEncoder {

    static final int BLOCK_SIZE = Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;
    private final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES];
    private final byte[] compressBlock = new byte[(int) Zstd.compressBound(BLOCK_SIZE * Long.BYTES)];

    @Override
    public void add(int index, long value) {
        assert index < BLOCK_SIZE;
        int pos = index * Long.BYTES;
        writeInt(pos, (int) (value >> 32));
        writeInt(pos + 4, (int) value);
    }

    private void writeInt(int index, int value) {
        buffer[index] = (byte) (value >> 24);
        buffer[index + 1] = (byte) (value >> 16);
        buffer[index + 2] = (byte) (value >> 8);
        buffer[index + 3] = (byte) value;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        int compressLength = ZstdUtil.compress(compressBlock, 0, compressBlock.length, buffer, 0, buffer.length, 1);
        out.writeVInt(compressLength);
        out.writeBytes(compressBlock, compressLength);
    }

    @Override
    public long get(int index) {
        int id = index * Long.BYTES;
        final int i1 =
                ((buffer[id] & 0xff) << 24)
                        | ((buffer[id + 1] & 0xff) << 16)
                        | ((buffer[id + 2] & 0xff) << 8)
                        | (buffer[id + 3] & 0xff);
        final int i2 =
                ((buffer[id + 4] & 0xff) << 24)
                        | ((buffer[id + 5] & 0xff) << 16)
                        | ((buffer[id + 6] & 0xff) << 8)
                        | (buffer[id + 7] & 0xff);
        return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
    }

    @Override
    public void decode(DataInput in) throws IOException {
        int compressedBlockLength = in.readVInt();
        in.readBytes(compressBlock, 0, compressedBlockLength);
        ZstdUtil.decompress(buffer, 0, buffer.length, compressBlock, 0, compressedBlockLength);
    }
}
