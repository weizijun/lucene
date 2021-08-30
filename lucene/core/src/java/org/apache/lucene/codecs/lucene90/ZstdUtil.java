package org.apache.lucene.codecs.lucene90;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdException;

public class ZstdUtil {
    public static int compress(byte[] dstBuff, int dstOffset, int dstSize, byte[] srcBuff, int srcOffset, int srcSize, int level) {
        ZstdCompressCtx ctx = new ZstdCompressCtx();
        try {
            ctx.setLevel(level);
            return ctx.compressByteArray(dstBuff, dstOffset, dstSize, srcBuff, srcOffset, srcSize);
        } finally {
            ctx.close();
        }
    }

    public static int decompress(byte[] dstBuff, int dstOffset, int dstSize, byte[] srcBuff, int srcOffset, int srcSize) throws ZstdException {
        ZstdDecompressCtx ctx = new ZstdDecompressCtx();
        try {
            return ctx.decompressByteArray(dstBuff, dstOffset, dstSize, srcBuff, srcOffset, srcSize);
        } finally {
            ctx.close();
        }
    }
}
