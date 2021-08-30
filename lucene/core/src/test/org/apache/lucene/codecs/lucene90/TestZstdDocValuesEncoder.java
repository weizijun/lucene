package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

public class TestZstdDocValuesEncoder extends LuceneTestCase {
    public void testRandomValues() throws IOException {
        long[] arr = new long[ZstdDocValuesEncoder.BLOCK_SIZE];
        for (int i = 0; i < ZstdDocValuesEncoder.BLOCK_SIZE; ++i) {
            arr[i] = random().nextLong();
        }
        doTest(arr, -1);
    }

    public void testTimeSeries() throws IOException {
        long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
        for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE / 4; i++) {
            arr[i] = NumericUtils.doubleToSortableLong(1.1);
        }

        for (int i = DocValuesEncoder.BLOCK_SIZE / 4; i < DocValuesEncoder.BLOCK_SIZE / 2; i++) {
            arr[i] = NumericUtils.doubleToSortableLong(2.2);
        }

        for (int i = DocValuesEncoder.BLOCK_SIZE / 2; i < 3 * DocValuesEncoder.BLOCK_SIZE / 4; i++) {
            arr[i] = NumericUtils.doubleToSortableLong(3.3);
        }

        for (int i = 3 * DocValuesEncoder.BLOCK_SIZE / 4; i < DocValuesEncoder.BLOCK_SIZE; i++) {
            arr[i] = NumericUtils.doubleToSortableLong(4.4);
        }
        final long expectedNumBytes = 47;
        doTest(arr, expectedNumBytes);
    }

    private void doTest(long[] arr, long expectedNumBytes) throws IOException {
        final long[] expected = arr.clone();
        ZstdDocValuesEncoder encoder = new ZstdDocValuesEncoder();
        for (int i = 0; i < expected.length; i++) {
            encoder.add(i, expected[i]);
        }
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                encoder.encode(out);
                if (expectedNumBytes != -1) {
                    assertEquals(expectedNumBytes, out.getFilePointer());
                }
            }
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                encoder.decode(in);
                assertEquals(in.length(), in.getFilePointer());

                long[] decoded = new long[expected.length];
                for (int i = 0; i < expected.length; i++) {
                    decoded[i] = encoder.get(i);
                }
                assertArrayEquals(expected, decoded);
            }
        }
    }
}
