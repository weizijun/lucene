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
package org.apache.lucene.backward_codecs.lucene99;

import static org.apache.lucene.backward_codecs.lucene99.Lucene99ScoreSkipReader.readImpacts;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.backward_codecs.lucene90.blocktree.FieldReader;
import org.apache.lucene.backward_codecs.lucene90.blocktree.Stats;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScoreSkipReader.MutableImpactList;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestLucene99PostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new Lucene99RWPostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  /** Make sure the final sub-block(s) are not skipped. */
  public void testFinalBlock() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
    for (int i = 0; i < 25; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
      doc.add(newStringField("field", "z" + Character.toString((char) (97 + i)), Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
    // We should see exactly two blocks: one root block (prefix empty string) and one block for z*
    // terms (prefix z):
    Stats stats = field.getStats();
    assertEquals(0, stats.floorBlockCount);
    assertEquals(2, stats.nonFloorBlockCount);
    r.close();
    w.close();
    d.close();
  }

  private void shouldFail(int minItemsInBlock, int maxItemsInBlock) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new Lucene99RWPostingsFormat(minItemsInBlock, maxItemsInBlock);
        });
  }

  public void testInvalidBlockSizes() throws Exception {
    shouldFail(0, 0);
    shouldFail(10, 8);
    shouldFail(-1, 10);
    shouldFail(10, -1);
    shouldFail(10, 12);
  }

  public void testImpactSerialization() throws IOException {
    // omit norms and omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 1L)));

    // omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 42L)));
    // omit freqs with very large norms
    doTestImpactSerialization(Collections.singletonList(new Impact(1, -100L)));

    // omit norms
    doTestImpactSerialization(Collections.singletonList(new Impact(30, 1L)));
    // omit norms with large freq
    doTestImpactSerialization(Collections.singletonList(new Impact(500, 1L)));

    // freqs and norms, basic
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(1, 7L),
            new Impact(3, 9L),
            new Impact(7, 10L),
            new Impact(15, 11L),
            new Impact(20, 13L),
            new Impact(28, 14L)));

    // freqs and norms, high values
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(2, 2L),
            new Impact(10, 10L),
            new Impact(12, 50L),
            new Impact(50, -100L),
            new Impact(1000, -80L),
            new Impact(1005, -3L)));
  }

  private void doTestImpactSerialization(List<Impact> impacts) throws IOException {
    CompetitiveImpactAccumulator acc = new CompetitiveImpactAccumulator();
    for (Impact impact : impacts) {
      acc.add(impact.freq, impact.norm);
    }
    try (Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
        Lucene99SkipWriter.writeImpacts(acc, out);
      }
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        byte[] b = new byte[Math.toIntExact(in.length())];
        in.readBytes(b, 0, b.length);
        List<Impact> impacts2 = readImpacts(new ByteArrayDataInput(b), new MutableImpactList());
        assertEquals(impacts, impacts2);
      }
    }
  }

  @Override
  protected void subCheckBinarySearch(TermsEnum termsEnum) throws Exception {
    // 10004a matched block's entries: [100001, 100003, ..., 100049].
    // if target greater than the last entry of the matched block,
    // termsEnum.term should be the next leaf block's first entry.
    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seekCeil(new BytesRef("10004a")));
    assertEquals(termsEnum.term(), new BytesRef("100051"));
  }
}
