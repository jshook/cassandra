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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.junit.Assert.assertEquals;

/**
 * Byte-identity guard for amortized PQ encoding (see {@link CassandraOnHeapGraph} and
 * {@code local/amortized-pq-encoding.md}). The amortized memtable flush replaces the flush-time
 * {@code encodeAll(...)} of the whole memtable with (1) encode-on-insert into a build-ordinal-indexed
 * {@link MutablePQVectors} and (2) a per-vector byte reorder into the flush's new (row-id) ordinal order.
 * The correctness claim is that this produces codes <em>byte-identical</em> to a direct
 * {@link ProductQuantization#encode} against the same codebook.
 * <p>
 * We fix a single codebook (PQ training via k-means is non-deterministic, so two independent full-stack
 * flushes cannot be byte-compared) and assert the identity for the three cases the flush hits: the fused
 * path (codes used directly, old ordinal), the non-fused path with no deletions (identity remap), and the
 * non-fused path with deletions (a remap with holes). {@link #reorderCodes} mirrors
 * {@link CassandraOnHeapGraph}'s private {@code reorderCodesForFlush} using only public jvector APIs, so a
 * mistake in the production {@code copyFrom} offsets or loop bounds is caught here against the independent
 * {@code encode} reference.
 */
public class AmortizedPqEncodingTest
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final int DIMENSION = 32;
    private static final int SUBSPACES = 8;   // number of subquantizers == compressed code bytes
    private static final int CLUSTERS = 256;
    private static final int N = 1024;        // enough vectors to train a PQ codebook

    private List<VectorFloat<?>> vectors;
    private ProductQuantization pq;
    private MutablePQVectors ingestCodes; // codes as built during amortized ingest, build-ordinal indexed
    private int codeBytes;

    @Before
    public void trainCodebookAndSimulateIngest()
    {
        // Deterministic vectors so a failure is reproducible; the codebook itself is trained once here and
        // reused for both the amortized codes and the reference encode within this run.
        Random rnd = new Random(42);
        vectors = new ArrayList<>(N);
        for (int i = 0; i < N; i++)
        {
            float[] f = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++)
                f[d] = rnd.nextFloat();
            vectors.add(vts.createFloatVector(f));
        }

        pq = ProductQuantization.compute(new ListRandomAccessVectorValues(vectors, DIMENSION),
                                         SUBSPACES, CLUSTERS, false);
        codeBytes = pq.compressedVectorSize();

        // Amortized ingest: each vector is encoded on insert into a build-ordinal-indexed store.
        ingestCodes = new MutablePQVectors(pq);
        for (int i = 0; i < N; i++)
            ingestCodes.encodeAndSet(i, vectors.get(i));
    }

    /** Fused path: the ingest codes are used directly by old (build) ordinal; each must equal a direct encode. */
    @Test
    public void ingestCodesMatchDirectEncode()
    {
        assertEquals(N, ingestCodes.count());
        for (int i = 0; i < N; i++)
            assertCodeEquals("ordinal " + i, pq.encode(vectors.get(i)), ingestCodes.get(i));
    }

    /** Non-fused, no deletions: an identity remap reorders to itself, still byte-identical to a direct encode. */
    @Test
    public void reorderIdentityMatchesEncode()
    {
        MutablePQVectors reordered = reorderCodes(ingestCodes, new OrdinalMapper.IdentityMapper(N - 1), pq);
        assertEquals(N, reordered.count());
        for (int i = 0; i < N; i++)
            assertCodeEquals("ordinal " + i, pq.encode(vectors.get(i)), reordered.get(i));
    }

    /**
     * Non-fused, with deletions: a row-id remap that both reorders survivors and leaves holes (mirroring
     * deleted rows in a memtable flush). Survivors must byte-match a direct encode; holes must be zeroed --
     * exactly what {@code encodeAll(RemappedVectorValues)} produces (a null vector encodes to a zeroed slot).
     */
    @Test
    public void reorderWithHolesMatchesEncode()
    {
        int[] newToOld = new int[N];
        for (int i = 0; i < N; i++)
            newToOld[i] = (N - 1) - i;              // reverse permutation, so ordering actually changes
        for (int i = 0; i < N; i += 7)
            newToOld[i] = OrdinalMapper.OMITTED;    // punch holes (deleted/omitted new ordinals)

        MutablePQVectors reordered = reorderCodes(ingestCodes, new ArrayOrdinalMapper(newToOld), pq);
        assertEquals(N, reordered.count());
        for (int newOrd = 0; newOrd < N; newOrd++)
        {
            int oldOrd = newToOld[newOrd];
            if (oldOrd == OrdinalMapper.OMITTED)
                assertCodeIsZero("hole at " + newOrd, reordered.get(newOrd));
            else
                assertCodeEquals("newOrd " + newOrd, pq.encode(vectors.get(oldOrd)), reordered.get(newOrd));
        }
    }

    /**
     * Mirrors {@link CassandraOnHeapGraph}'s {@code reorderCodesForFlush}: reorder build-ordinal ingest codes
     * into new (row-id) ordinal order by copying each surviving code with {@code copyFrom(src, 0, 0, len)}
     * (view-relative slice offsets) and zeroing holes, producing {@code maxOrdinal + 1} entries.
     */
    private static MutablePQVectors reorderCodes(MutablePQVectors src, OrdinalMapper mapper, ProductQuantization pq)
    {
        int codeBytes = pq.compressedVectorSize();
        MutablePQVectors out = new MutablePQVectors(pq);
        for (int newOrd = 0; newOrd <= mapper.maxOrdinal(); newOrd++)
        {
            out.setZero(newOrd); // allocate + zero (matches encodeAll's null -> zeroed slot)
            int oldOrd = mapper.newToOld(newOrd);
            if (oldOrd != OrdinalMapper.OMITTED)
                out.get(newOrd).copyFrom(src.get(oldOrd), 0, 0, codeBytes);
        }
        return out;
    }

    private void assertCodeEquals(String msg, ByteSequence<?> expected, ByteSequence<?> actual)
    {
        for (int j = 0; j < codeBytes; j++)
            assertEquals(msg + " byte " + j, expected.get(j), actual.get(j));
    }

    private void assertCodeIsZero(String msg, ByteSequence<?> actual)
    {
        for (int j = 0; j < codeBytes; j++)
            assertEquals(msg + " byte " + j, (byte) 0, actual.get(j));
    }

    /** An {@link OrdinalMapper} backed by an explicit newToOld array (oldToNew is the inverse, unused here). */
    private static final class ArrayOrdinalMapper implements OrdinalMapper
    {
        private final int[] newToOld;

        ArrayOrdinalMapper(int[] newToOld)
        {
            this.newToOld = newToOld;
        }

        @Override
        public int maxOrdinal()
        {
            return newToOld.length - 1;
        }

        @Override
        public int newToOld(int newOrdinal)
        {
            return newToOld[newOrdinal];
        }

        @Override
        public int oldToNew(int oldOrdinal)
        {
            for (int i = 0; i < newToOld.length; i++)
                if (newToOld[i] == oldOrdinal)
                    return i;
            return OrdinalMapper.OMITTED;
        }
    }
}
