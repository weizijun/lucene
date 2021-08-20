/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

/**
 * first use delta encode, then compressed.
 */
public class DeltaLZ4DocValuesEncoder implements BaseEncoder {

    static final int BLOCK_SIZE = Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;
    private LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();

    private final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES];
    private final long[] longs = new long[BLOCK_SIZE];

    @Override
    public void add(int index, long value) {
        assert index < BLOCK_SIZE;
        longs[index] = value;
    }

    private void writeLong(int index, long value) {
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
        long preValue = 0;
        for (int i = 0; i < longs.length; ++i) {
            writeLong(i, longs[i] - preValue);
            preValue = longs[i];
        }
        LZ4.compress(buffer, 0, buffer.length, out, ht);
    }

    @Override
    public long get(int index) {
        return longs[index];
    }

    private long readLong(int index) {
        int id = index * Long.BYTES;
        final int i1 = ((buffer[id] & 0xff) << 24) | ((buffer[id + 1] & 0xff) << 16) | ((buffer[id + 2]
                & 0xff) << 8) | (buffer[id + 3] & 0xff);
        final int i2 = ((buffer[id + 4] & 0xff) << 24) | ((buffer[id + 5] & 0xff) << 16) | ((buffer[id
                + 6] & 0xff) << 8) | (buffer[id + 7] & 0xff);
        return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
    }

    @Override
    public void decode(DataInput in) throws IOException {
        LZ4.decompress(in, buffer.length, buffer, 0);
        long preValue = 0;
        for (int i = 0; i < buffer.length / Long.BYTES; i++) {
            long value = readLong(i) + preValue;
            longs[i] = value;
            preValue = value;
        }
    }
}
