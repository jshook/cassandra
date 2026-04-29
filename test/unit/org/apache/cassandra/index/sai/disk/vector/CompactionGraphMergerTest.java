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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CompactionGraphMerger}'s internal helpers —
 * {@link CompactionGraphMerger#buildLiveBitset(OnDiskGraphIndex)},
 * {@link CompactionGraphMerger#buildLiveBitset(OnDiskGraphIndex, OnDiskGraphIndex.View, Map)},
 * and {@link CompactionGraphMerger.MergedSourceVectorValues} — exercised without
 * any Cassandra cluster, schema, or SSTable infrastructure.
 *
 * Graphs are built with jvector's {@link GraphIndexBuilder} and written to a
 * temp directory so that {@link OnDiskGraphIndex} instances can be loaded and
 * passed directly into the merger helpers.
 */
public class CompactionGraphMergerTest
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final ForkJoinPool fjp = ForkJoinPool.commonPool();

    private static final int DIM = 4;
    private static final VectorSimilarityFunction VSF = VectorSimilarityFunction.EUCLIDEAN;

    private Path tempDir;

    @Before
    public void setUp() throws IOException
    {
        tempDir = Files.createTempDirectory("CompactionGraphMergerTest");
    }

    @After
    public void tearDown() throws IOException
    {
        if (Files.exists(tempDir))
        {
            try (var stream = Files.walk(tempDir))
            {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    // -------------------------------------------------------------------------
    // buildLiveBitset(graph) — no gaps (fast path)
    // -------------------------------------------------------------------------

    /**
     * A freshly-flushed segment has no tombstones: {@code idUpperBound == size(0)}.
     * {@code buildLiveBitset} should return a bitset of length {@code size(0)} with every
     * bit set and {@code cardinality == size(0)}.
     */
    @Test
    public void testBuildLiveBitsetNoGaps() throws IOException
    {
        int n = 8;
        OnDiskGraphIndex graph = buildCleanGraph(makeVectors(n, 0), "no_gaps");

        assertEquals(n, graph.size(0));
        assertEquals("no tombstones expected", n, graph.getIdUpperBound());

        FixedBitSet bs = CompactionGraphMerger.buildLiveBitset(graph);

        assertEquals("bitset length should equal idUpperBound", n, bs.length());
        assertEquals("cardinality should equal liveCount", n, bs.cardinality());
        for (int i = 0; i < n; i++)
            assertTrue("bit " + i + " should be set", bs.get(i));
    }

    // -------------------------------------------------------------------------
    // buildLiveBitset(graph) — with gaps (slow path)
    // -------------------------------------------------------------------------

    /**
     * A segment with one tombstoned slot has {@code idUpperBound == size(0) + 1}.
     * The largest live node ID equals {@code size(0)}, which is exactly one past the end of a
     * bitset sized to {@code size(0)}, triggering the pre-fix {@link FixedBitSet} assertion.
     * <p>
     * {@code buildLiveBitset} must size the bitset to {@code idUpperBound} and mark only the
     * actual live node IDs returned by {@link OnDiskGraphIndex#getNodes(int)}.
     */
    @Test
    public void testBuildLiveBitsetWithGaps() throws IOException
    {
        int n = 6;  // 6 live nodes; gap written at slot 2 → idUpperBound = 7, size(0) = 6
        OnDiskGraphIndex graph = buildGraphWithGapAtSlot(makeVectors(n, 0), 2, "with_gaps");

        int liveCount = graph.size(0);
        int idBound = graph.getIdUpperBound();

        assertEquals(n, liveCount);
        assertTrue("idUpperBound must exceed liveCount when gaps exist", idBound > liveCount);

        FixedBitSet bs = CompactionGraphMerger.buildLiveBitset(graph);

        assertEquals("bitset length must equal idUpperBound", idBound, bs.length());
        assertEquals("cardinality must equal number of live nodes", liveCount, bs.cardinality());
        assertFalse("tombstoned slot 2 must not be set", bs.get(2));

        // Every node ID returned by getNodes(0) must be set in the bitset
        var nodeIt = graph.getNodes(0);
        while (nodeIt.hasNext())
        {
            int nodeId = nodeIt.nextInt();
            assertTrue("live node " + nodeId + " must be set in bitset", bs.get(nodeId));
        }
    }

    /**
     * Regression: the old code created {@code new FixedBitSet(size(0))}, which fails with an
     * assertion error when any live node has ID {@code >= size(0)}.  With one gap, the maximum
     * live node ID is exactly {@code size(0)}, reliably triggering the bug.
     *
     * This test documents the failure mode and confirms the new code handles it correctly.
     */
    @Test
    public void testOldBitsetSizeWouldFailOnGappedGraph() throws IOException
    {
        int n = 4;  // 4 live nodes, gap at slot 1 → live IDs = {0, 2, 3, 4}
        OnDiskGraphIndex graph = buildGraphWithGapAtSlot(makeVectors(n, 1), 1, "gap_regression");

        int liveCount = graph.size(0);
        int idBound = graph.getIdUpperBound();

        assertEquals(n, liveCount);
        assertTrue("test requires idUpperBound > liveCount", idBound > liveCount);

        // Collect the maximum live node ID
        var nodeIt = graph.getNodes(0);
        int maxNodeId = -1;
        while (nodeIt.hasNext())
            maxNodeId = Math.max(maxNodeId, nodeIt.nextInt());

        // With exactly one gap, the max node ID equals liveCount (= size(0))
        assertEquals("max live node ID must equal liveCount", liveCount, maxNodeId);

        // Demonstrate that the old bitset size is insufficient
        FixedBitSet oldStyleBitset = new FixedBitSet(liveCount);
        // Attempting oldStyleBitset.get(maxNodeId) would fire the FixedBitSet assertion
        // (index == numBits), so we verify this by inspecting the length only:
        assertEquals("old bitset length equals liveCount, not enough to hold maxNodeId",
                     liveCount, oldStyleBitset.length());
        assertTrue("maxNodeId is out of range for old bitset", maxNodeId >= oldStyleBitset.length());

        // The new code handles it without error
        FixedBitSet newBitset = CompactionGraphMerger.buildLiveBitset(graph);
        assertTrue("new bitset must accept max live node ID", maxNodeId < newBitset.length());
        assertTrue("new bitset must mark max live node ID as live", newBitset.get(maxNodeId));
    }

    // -------------------------------------------------------------------------
    // buildLiveBitset(graph, view, postingsMap) — dead node tracking
    // -------------------------------------------------------------------------

    /**
     * When only a subset of vectors appear in the postings map, {@code buildLiveBitset}
     * must mark only those as live and exclude nodes whose rows were deleted.
     */
    @Test
    public void testBuildLiveBitsetDeadNodesExcluded() throws IOException
    {
        List<VectorFloat<?>> vecs = makeDistinctVectors(6, 100.0f);
        OnDiskGraphIndex graph = buildCleanGraph(vecs, "dead_nodes");

        // Put only vectors 0, 2, 4 into the "postings map" (vectors 1, 3, 5 were deleted)
        Map<VectorFloat<?>, Object> postingsMap = new HashMap<>();
        postingsMap.put(vecs.get(0), Boolean.TRUE);
        postingsMap.put(vecs.get(2), Boolean.TRUE);
        postingsMap.put(vecs.get(4), Boolean.TRUE);

        try (var view = graph.getView())
        {
            FixedBitSet bs = CompactionGraphMerger.buildLiveBitset(graph, view, postingsMap);

            assertEquals("bitset length must equal idUpperBound", graph.getIdUpperBound(), bs.length());
            assertEquals("only surviving nodes should be live", 3, bs.cardinality());
            assertTrue("node 0 should be live", bs.get(0));
            assertFalse("node 1 should be dead", bs.get(1));
            assertTrue("node 2 should be live", bs.get(2));
            assertFalse("node 3 should be dead", bs.get(3));
            assertTrue("node 4 should be live", bs.get(4));
            assertFalse("node 5 should be dead", bs.get(5));
        }
    }

    /**
     * With a gapped graph (jvector tombstones) AND dead Cassandra nodes, the bitset must
     * exclude both tombstoned slots and rows that did not survive compaction.
     */
    @Test
    public void testBuildLiveBitsetGapAndDeadNodes() throws IOException
    {
        int n = 5;
        List<VectorFloat<?>> vecs = makeDistinctVectors(n, 200.0f);
        // Gap at slot 1 → live node IDs are {0, 2, 3, 4, 5}, idUpperBound = 6
        OnDiskGraphIndex graph = buildGraphWithGapAtSlot(vecs, 1, "gap_and_dead");

        // Only the vectors at node IDs 0 and 3 survive (Cassandra-level deletion)
        // We need to know which vector lands at which node ID. With gapSlot=1:
        // old ordinals 0,1,2,3,4 → new ordinals 0,2,3,4,5 (slot 1 is OMITTED)
        // So: node ID 0 → vecs[0], node ID 2 → vecs[1], node ID 3 → vecs[2],
        //     node ID 4 → vecs[3], node ID 5 → vecs[4]
        Map<VectorFloat<?>, Object> postingsMap = new HashMap<>();
        postingsMap.put(vecs.get(0), Boolean.TRUE); // node ID 0
        postingsMap.put(vecs.get(2), Boolean.TRUE); // node ID 3

        try (var view = graph.getView())
        {
            FixedBitSet bs = CompactionGraphMerger.buildLiveBitset(graph, view, postingsMap);

            assertEquals("bitset length must equal idUpperBound", graph.getIdUpperBound(), bs.length());
            // 2 Cassandra-surviving nodes
            assertEquals("exactly 2 surviving nodes", 2, bs.cardinality());
            assertTrue("node 0 (vecs[0]) should be live", bs.get(0));
            // slot 1 was tombstoned by the gap mapper
            assertFalse("slot 1 was tombstoned, must not be set", bs.get(1));
            // node ID 3 → vecs[2] survives
            assertTrue("node 3 (vecs[2]) should be live", bs.get(3));
        }
    }

    /**
     * A full postings map (every vector present) must produce a bitset equivalent to the
     * jvector-only {@code buildLiveBitset(graph)} result.
     */
    @Test
    public void testBuildLiveBitsetAllAliveEqualsJvectorOnly() throws IOException
    {
        int n = 5;
        List<VectorFloat<?>> vecs = makeDistinctVectors(n, 300.0f);
        OnDiskGraphIndex graph = buildCleanGraph(vecs, "all_alive");

        Map<VectorFloat<?>, Object> allPresent = new HashMap<>();
        for (var v : vecs)
            allPresent.put(v, Boolean.TRUE);

        try (var view = graph.getView())
        {
            FixedBitSet jvectorBs = CompactionGraphMerger.buildLiveBitset(graph);
            FixedBitSet deadAwareBs = CompactionGraphMerger.buildLiveBitset(graph, view, allPresent);

            assertEquals("cardinality must match", jvectorBs.cardinality(), deadAwareBs.cardinality());
            assertEquals("length must match", jvectorBs.length(), deadAwareBs.length());
            for (int i = 0; i < n; i++)
                assertEquals("bit " + i + " must agree", jvectorBs.get(i), deadAwareBs.get(i));
        }
    }

    // -------------------------------------------------------------------------
    // MergedSourceVectorValues — sizes constructor (backward compat)
    // -------------------------------------------------------------------------

    @Test
    public void testMergedSourceSizeAndDimension() throws IOException
    {
        int n0 = 5, n1 = 7, n2 = 3;
        OnDiskGraphIndex g0 = buildCleanGraph(makeVectors(n0, 0), "mv_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(makeVectors(n1, 1), "mv_g1");
        OnDiskGraphIndex g2 = buildCleanGraph(makeVectors(n2, 2), "mv_g2");

        try (var v0 = g0.getView(); var v1 = g1.getView(); var v2 = g2.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    new int[]{ n0, n1, n2 }, DIM, List.of(v0, v1, v2));

            assertEquals(n0 + n1 + n2, merged.size());
            assertEquals(DIM, merged.dimension());
        }
    }

    /**
     * Vectors from source 0 occupy global ordinals [0, n0) and vectors from source 1 occupy
     * [n0, n0+n1).  Checks that {@code getVector} returns the exact expected values for
     * every global ordinal.
     */
    @Test
    public void testVectorLookupCorrectness() throws IOException
    {
        int n0 = 3, n1 = 4;
        List<VectorFloat<?>> vecs0 = makeDistinctVectors(n0, 100.0f);
        List<VectorFloat<?>> vecs1 = makeDistinctVectors(n1, 200.0f);

        OnDiskGraphIndex g0 = buildCleanGraph(vecs0, "lk_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(vecs1, "lk_g1");

        try (var v0 = g0.getView(); var v1 = g1.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    new int[]{ n0, n1 }, DIM, List.of(v0, v1));

            for (int i = 0; i < n0; i++)
                assertEquals("source-0 ordinal " + i,
                             vecs0.get(i).get(0), merged.getVector(i).get(0), 0.0f);

            for (int i = 0; i < n1; i++)
                assertEquals("source-1 ordinal " + i,
                             vecs1.get(i).get(0), merged.getVector(n0 + i).get(0), 0.0f);
        }
    }

    /**
     * Checks the exact boundary between two sources: the last ordinal of source 0 and the
     * first ordinal of source 1.  Off-by-one errors in the mapping would produce the wrong
     * vector here.
     */
    @Test
    public void testSourceBoundaryOrdinals() throws IOException
    {
        int n0 = 5, n1 = 5;
        List<VectorFloat<?>> vecs0 = makeDistinctVectors(n0, 10.0f);
        List<VectorFloat<?>> vecs1 = makeDistinctVectors(n1, 20.0f);

        OnDiskGraphIndex g0 = buildCleanGraph(vecs0, "bd_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(vecs1, "bd_g1");

        try (var v0 = g0.getView(); var v1 = g1.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    new int[]{ n0, n1 }, DIM, List.of(v0, v1));

            VectorFloat<?> buf = vts.createFloatVector(DIM);

            // Last ordinal of source 0
            merged.getVectorInto(n0 - 1, buf, 0);
            assertEquals("last ordinal of source 0",
                         vecs0.get(n0 - 1).get(0), buf.get(0), 0.0f);

            // First ordinal of source 1
            merged.getVectorInto(n0, buf, 0);
            assertEquals("first ordinal of source 1",
                         vecs1.get(0).get(0), buf.get(0), 0.0f);
        }
    }

    /**
     * Three sources of different sizes: verifies that all source boundaries are resolved
     * correctly.
     */
    @Test
    public void testFindSourceWithMultipleSources() throws IOException
    {
        int n0 = 2, n1 = 5, n2 = 3;
        List<VectorFloat<?>> vecs0 = makeDistinctVectors(n0, 1000.0f);
        List<VectorFloat<?>> vecs1 = makeDistinctVectors(n1, 2000.0f);
        List<VectorFloat<?>> vecs2 = makeDistinctVectors(n2, 3000.0f);

        OnDiskGraphIndex g0 = buildCleanGraph(vecs0, "fs_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(vecs1, "fs_g1");
        OnDiskGraphIndex g2 = buildCleanGraph(vecs2, "fs_g2");

        try (var v0 = g0.getView(); var v1 = g1.getView(); var v2 = g2.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    new int[]{ n0, n1, n2 }, DIM, List.of(v0, v1, v2));

            // Verify every global ordinal resolves to the right source's vector
            List<VectorFloat<?>> all = new ArrayList<>();
            all.addAll(vecs0);
            all.addAll(vecs1);
            all.addAll(vecs2);

            for (int g = 0; g < all.size(); g++)
                assertEquals("global ordinal " + g,
                             all.get(g).get(0), merged.getVector(g).get(0), 0.0f);
        }
    }

    // -------------------------------------------------------------------------
    // MergedSourceVectorValues — direct-array constructor (production path)
    // -------------------------------------------------------------------------

    /**
     * The production constructor accepts explicit {@code globalToSrcIdx} and
     * {@code globalToNodeId} arrays.  This test verifies it returns the correct vectors
     * when some source nodes are intentionally skipped (dead nodes).
     */
    @Test
    public void testMergedSourceDirectArrayConstructor() throws IOException
    {
        // Source 0: vectors [100, 101, 102]; source 1: vectors [200, 201, 202]
        List<VectorFloat<?>> vecs0 = makeDistinctVectors(3, 100.0f);
        List<VectorFloat<?>> vecs1 = makeDistinctVectors(3, 200.0f);

        OnDiskGraphIndex g0 = buildCleanGraph(vecs0, "da_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(vecs1, "da_g1");

        // Simulate dead nodes: include only node 0 from source 0 and nodes 1,2 from source 1.
        // Global ordinals: 0 → (src=0, nid=0), 1 → (src=1, nid=1), 2 → (src=1, nid=2)
        int[] globalToSrcIdx = { 0, 1, 1 };
        int[] globalToNodeId = { 0, 1, 2 };

        try (var v0 = g0.getView(); var v1 = g1.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    globalToSrcIdx, globalToNodeId, DIM, List.of(v0, v1));

            assertEquals("total ordinals", 3, merged.size());
            assertEquals("dimension", DIM, merged.dimension());

            // Global ordinal 0 → source 0, node 0 → vecs0.get(0)
            assertEquals("ordinal 0 (src0, nid0)",
                         vecs0.get(0).get(0), merged.getVector(0).get(0), 0.0f);
            // Global ordinal 1 → source 1, node 1 → vecs1.get(1)
            assertEquals("ordinal 1 (src1, nid1)",
                         vecs1.get(1).get(0), merged.getVector(1).get(0), 0.0f);
            // Global ordinal 2 → source 1, node 2 → vecs1.get(2)
            assertEquals("ordinal 2 (src1, nid2)",
                         vecs1.get(2).get(0), merged.getVector(2).get(0), 0.0f);
        }
    }

    /**
     * Verifies that {@code getVectorInto} works correctly with the direct-array constructor.
     */
    @Test
    public void testMergedSourceDirectArrayGetVectorInto() throws IOException
    {
        List<VectorFloat<?>> vecs0 = makeDistinctVectors(2, 500.0f);
        List<VectorFloat<?>> vecs1 = makeDistinctVectors(2, 600.0f);

        OnDiskGraphIndex g0 = buildCleanGraph(vecs0, "gi_g0");
        OnDiskGraphIndex g1 = buildCleanGraph(vecs1, "gi_g1");

        // Skip node 1 from source 0 (dead); global ordinals: 0→(0,0), 1→(1,0), 2→(1,1)
        int[] globalToSrcIdx = { 0, 1, 1 };
        int[] globalToNodeId = { 0, 0, 1 };

        try (var v0 = g0.getView(); var v1 = g1.getView())
        {
            var merged = new CompactionGraphMerger.MergedSourceVectorValues(
                    globalToSrcIdx, globalToNodeId, DIM, List.of(v0, v1));

            VectorFloat<?> buf = vts.createFloatVector(DIM);

            merged.getVectorInto(0, buf, 0);
            assertEquals("ordinal 0 → src0, nid0", vecs0.get(0).get(0), buf.get(0), 0.0f);

            merged.getVectorInto(2, buf, 0);
            assertEquals("ordinal 2 → src1, nid1", vecs1.get(1).get(0), buf.get(0), 0.0f);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns {@code n} vectors where vector {@code i} has {@code base + i} in dimension 0 and
     * zeros elsewhere, so each has a distinct, predictable first component.
     */
    private List<VectorFloat<?>> makeDistinctVectors(int n, float base)
    {
        List<VectorFloat<?>> vecs = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
        {
            float[] data = new float[DIM];
            data[0] = base + i;
            vecs.add(vts.createFloatVector(data));
        }
        return vecs;
    }

    /** Returns {@code n} varied vectors using {@code seed} as a variation parameter. */
    private List<VectorFloat<?>> makeVectors(int n, int seed)
    {
        List<VectorFloat<?>> vecs = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
        {
            float[] data = new float[DIM];
            for (int d = 0; d < DIM; d++)
                data[d] = (float) Math.sin(seed * 31 + i * 17 + d * 7);
            vecs.add(vts.createFloatVector(data));
        }
        return vecs;
    }

    /**
     * Builds and loads a clean on-disk graph from {@code vecs}: no tombstones, so
     * {@code idUpperBound == size(0) == vecs.size()}.
     */
    private OnDiskGraphIndex buildCleanGraph(List<VectorFloat<?>> vecs, String name)
            throws IOException
    {
        int n = vecs.size();
        RandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vecs, DIM);
        var bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, VSF);
        var builder = new GraphIndexBuilder(bsp, DIM, 4, 20, 1.2f, 1.2f, false, true, fjp, fjp);
        for (int i = 0; i < n; i++)
            builder.addGraphNode(i, vecs.get(i));
        builder.cleanup();

        Map<FeatureId, IntFunction<Feature.State>> suppliers = new EnumMap<>(FeatureId.class);
        suppliers.put(FeatureId.INLINE_VECTORS, ord -> new InlineVectors.State(ravv.getVector(ord)));

        Path out = tempDir.resolve(name);
        try (var writer = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), out)
                .withMapper(new OrdinalMapper.IdentityMapper(n - 1))
                .with(new InlineVectors(DIM))
                .build())
        {
            writer.write(suppliers);
        }
        return OnDiskGraphIndex.load(ReaderSupplierFactory.open(out));
    }

    /**
     * Builds and loads a graph with {@code n} live nodes but a tombstone at {@code gapSlot},
     * so {@code idUpperBound == n + 1} and {@code size(0) == n}.
     *
     * The mapper skips new-ordinal {@code gapSlot}: new ordinals [0, gapSlot) map to old
     * ordinals [0, gapSlot); gapSlot maps to OMITTED (tombstone); new ordinals
     * [gapSlot+1, n] map to old ordinals [gapSlot, n-1].  Written node IDs are therefore
     * {0, …, gapSlot-1, gapSlot+1, …, n}, making the maximum live node ID equal to
     * {@code n == size(0)}, which is exactly the index that overflows the old bitset.
     */
    private OnDiskGraphIndex buildGraphWithGapAtSlot(List<VectorFloat<?>> vecs, int gapSlot,
                                                     String name) throws IOException
    {
        int n = vecs.size();
        RandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vecs, DIM);
        var bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, VSF);
        var builder = new GraphIndexBuilder(bsp, DIM, 4, 20, 1.2f, 1.2f, false, true, fjp, fjp);
        for (int i = 0; i < n; i++)
            builder.addGraphNode(i, vecs.get(i));
        builder.cleanup();

        // maxOrdinal = n → n+1 slots (0..n); slot gapSlot is OMITTED (tombstone written by writer)
        OrdinalMapper gapMapper = new OrdinalMapper()
        {
            @Override public int maxOrdinal() { return n; }

            @Override
            public int oldToNew(int old)
            {
                return old < gapSlot ? old : old + 1;
            }

            @Override
            public int newToOld(int newOrd)
            {
                if (newOrd == gapSlot) return OMITTED;
                return newOrd < gapSlot ? newOrd : newOrd - 1;
            }
        };

        Map<FeatureId, IntFunction<Feature.State>> suppliers = new EnumMap<>(FeatureId.class);
        suppliers.put(FeatureId.INLINE_VECTORS, ord -> new InlineVectors.State(ravv.getVector(ord)));

        Path out = tempDir.resolve(name);
        try (var writer = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), out)
                .withMapper(gapMapper)
                .with(new InlineVectors(DIM))
                .build())
        {
            writer.write(suppliers);
        }
        return OnDiskGraphIndex.load(ReaderSupplierFactory.open(out));
    }
}
