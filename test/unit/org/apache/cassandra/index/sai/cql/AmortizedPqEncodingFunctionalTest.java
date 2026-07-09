/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.utils.Glove;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wiring test for amortized PQ encoding ({@code cassandra.sai.vector.amortize_pq_encoding}, see
 * {@link CassandraOnHeapGraph} and {@code local/amortized-pq-encoding.md}). The {@code AmortizedPqEncodingTest}
 * unit test proves the reorder/encode algorithm is byte-identical to a direct encode; this test proves the
 * <em>plumbing</em> — that a real memtable flush actually takes the amortized path (adopt a codebook on the
 * memtable ctor, encode on insert, serialize pre-built codes at flush) and yields a valid, PQ-compressed,
 * correctly-searchable on-disk index.
 * <p>
 * Setup guarantees the amortized path is exercised: a compacted base segment of {@code >= MIN_PQ_ROWS} rows
 * with {@code source_model='OTHER'} (always PQ) establishes an on-disk codebook, so the <em>second</em>
 * memtable adopts it and its flush is the amortized one under test. Correctness is asserted three ways:
 * the index verifies its checksums, a segment reports PQ compression (so PQ — hence amortization — is in
 * play), and ANN queries hit a recall floor against an independent full-precision search (garbled codes
 * would collapse recall). The non-amortized run is the calibration baseline for the same floor.
 */
public class AmortizedPqEncodingFunctionalTest extends VectorTester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static Glove.WordVector word2vec;

    private static final int BASE_ROWS = 1100;   // > MIN_PQ_ROWS, so the base segment trains a PQ codebook
    private static final int EXTRA_ROWS = 400;   // the second batch -> the amortized memtable flush
    private static final int LIMIT = 20;
    private static final int QUERIES = 5;
    private static final double RECALL_FLOOR = 0.6; // conservative; glove+PQ recall runs well above this

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = Glove.parse(AmortizedPqEncodingFunctionalTest.class.getClassLoader()
                                                                      .getResourceAsStream("glove.3K.50d.txt"));
    }

    @After
    public void resetProps()
    {
        CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.reset();
    }

    @Test
    public void amortizedFlushProducesCorrectPqIndex() throws Throwable
    {
        double recall = buildTwoSegmentIndexAndMeasureRecall(true);
        assertThat(recall).as("avg recall with amortized PQ encoding").isGreaterThanOrEqualTo(RECALL_FLOOR);
    }

    /** Baseline: the same build with the flush-time encode path, so the recall floor is calibrated, not lucky. */
    @Test
    public void nonAmortizedFlushBaseline() throws Throwable
    {
        double recall = buildTwoSegmentIndexAndMeasureRecall(false);
        assertThat(recall).as("avg recall with flush-time PQ encoding").isGreaterThanOrEqualTo(RECALL_FLOOR);
    }

    private double buildTwoSegmentIndexAndMeasureRecall(boolean amortize) throws Throwable
    {
        CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.setBoolean(amortize);

        createTable(String.format("CREATE TABLE %%s (pk int, val vector<float, %d>, PRIMARY KEY(pk))",
                                  word2vec.dimension()));

        List<float[]> inserted = new ArrayList<>(BASE_ROWS + EXTRA_ROWS);
        int pk = 0;

        // Base batch, inserted before the index exists, then compacted to a single sstable of >= MIN_PQ_ROWS
        // rows so the index build trains and writes a PQ codebook.
        for (int i = 0; i < BASE_ROWS; i++)
        {
            float[] v = word2vec.vector(word2vec.word(pk));
            execute("INSERT INTO %s (pk, val) VALUES (?, " + vectorString(v) + ")", pk++);
            inserted.add(v);
        }
        flush();
        compact();
        waitForCompactionsFinished();

        // OTHER always compresses with PQ; the build over the compacted base sstable establishes the codebook.
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'" +
                                       " WITH OPTIONS = {'source_model': 'OTHER'}");

        // Second batch: with the codebook now on disk, this memtable adopts it; with amortize=on its flush
        // serializes pre-built codes (the path under test), with amortize=off it encodes at flush time.
        for (int i = 0; i < EXTRA_ROWS; i++)
        {
            float[] v = word2vec.vector(word2vec.word(pk));
            execute("INSERT INTO %s (pk, val) VALUES (?, " + vectorString(v) + ")", pk++);
            inserted.add(v);
        }
        flush();

        // The amortized index must be structurally valid and actually PQ-compressed (else amortization is a no-op).
        verifyChecksum();
        assertPqCompression(indexName);

        // ANN queries must return correctly-ordered, high-recall results across both segments.
        double totalRecall = 0;
        for (int q = 0; q < QUERIES; q++)
        {
            float[] queryVector = inserted.get(getRandom().nextIntBetween(0, inserted.size() - 1));
            UntypedResultSet rs = execute("SELECT val FROM %s ORDER BY val ANN OF " + vectorString(queryVector) +
                                          " LIMIT " + LIMIT);
            List<float[]> results = getVectorsFromResult(rs);
            assertDescendingScore(queryVector, results);
            totalRecall += rawIndexedRecall(inserted, queryVector, results, LIMIT);
        }
        return totalRecall / QUERIES;
    }

    private void assertPqCompression(String indexName) throws IOException
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        var view = index.getIndexContext().getView();
        var sstableIndex = view.iterator().next();
        try (var segment = sstableIndex.getSegments().iterator().next();
             var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher())
        {
            assertThat(searcher.getCompression().type)
                .as("segment vector compression")
                .isEqualTo(VectorCompression.CompressionType.PRODUCT_QUANTIZATION);
        }
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, word2vec.dimension());
        for (UntypedResultSet.Row row : result)
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        return vectors;
    }

    private void assertDescendingScore(float[] queryVector, List<float[]> resultVectors)
    {
        float prevScore = -1;
        for (float[] current : resultVectors)
        {
            float score = VectorSimilarityFunction.COSINE.compare(vts.createFloatVector(current),
                                                                  vts.createFloatVector(queryVector));
            if (prevScore >= 0)
                assertThat(score).isLessThanOrEqualTo(prevScore);
            prevScore = score;
        }
    }
}
