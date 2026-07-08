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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.util.work.ProgressLimiter;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.RemappedPostings;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static java.util.stream.Collectors.toList;

/**
 * Merges multiple existing on-disk HNSW graph segments into a single compacted segment using
 * jvector's {@link OnDiskGraphIndexCompactor}. This is the graph-index side of Cassandra's LSM
 * compaction: instead of rebuilding the graph from individual vectors, we merge existing on-disk
 * graph indexes directly, which produces higher-quality results without a full rebuild.
 *
 * <p>Each source is represented by a {@link SourceSegment} containing the on-disk graph and its
 * associated {@link CassandraDiskAnn} (for PQ, unit-vectors flag, and the ordinals map).
 *
 * <p>Supported source feature sets:
 * <ul>
 *   <li>{@code INLINE_VECTORS} only: dead-node detection uses full-precision vector lookup in the
 *       postings map. The PQ component is written with {@link CompressionType#NONE}.
 *   <li>{@code INLINE_VECTORS} + {@code FUSED_PQ}: dead-node detection uses full-precision vector
 *       lookup. After compaction, the retrained PQ codebook is read back from the compacted graph
 *       and written to the Cassandra PQ component so query encoding uses the correct codebook.
 * </ul>
 *
 * <p>Any other feature set (NVQ, separated vectors, non-fused PQ) causes {@link #merge} to throw
 * {@link IllegalStateException} so the caller can fall back to the legacy graph-rebuild path.
 */
public class CompactionGraphMerger
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraphMerger.class);

    /** Killswitch: set to false to fall back to the legacy graph-rebuild path without restarting. */
    public static volatile boolean ENABLED = CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean();

    /**
     * One input segment for the merge. The {@link CassandraDiskAnn} provides the on-disk graph
     * (via {@link CassandraDiskAnn#getOnDiskGraph()}), PQ, and other metadata.
     */
    public static final class SourceSegment
    {
        private final CassandraDiskAnn diskAnn;
        private final long segmentRowIdOffset;

        public SourceSegment(CassandraDiskAnn diskAnn, long segmentRowIdOffset)
        {
            this.diskAnn = diskAnn;
            this.segmentRowIdOffset = segmentRowIdOffset;
        }

        public CassandraDiskAnn diskAnn()
        {
            return diskAnn;
        }

        public long segmentRowIdOffset()
        {
            return segmentRowIdOffset;
        }

        public OnDiskGraphIndex graph()
        {
            return diskAnn.getOnDiskGraph();
        }
    }

    private final List<SourceSegment> sources;
    private final VectorSimilarityFunction similarityFunction;
    private final IndexComponents.ForWrite perIndexComponents;
    private final int dimension;

    public CompactionGraphMerger(List<SourceSegment> sources,
                                 VectorSimilarityFunction similarityFunction,
                                 IndexComponents.ForWrite perIndexComponents)
    {
        if (sources.size() < 2)
            throw new IllegalArgumentException("At least 2 source segments required for graph merging; got " + sources.size());
        this.sources = sources;
        this.similarityFunction = similarityFunction;
        this.perIndexComponents = perIndexComponents;
        this.dimension = sources.get(0).graph().getDimension();
    }

    /**
     * Merges the source graphs and writes all SAI index components for the output segment.
     *
     * <p>Only sources with {@code INLINE_VECTORS} (with or without {@code FUSED_PQ}) are
     * supported. Any other feature set causes an {@link IllegalStateException} so the caller
     * can fall back to the legacy graph-rebuild path.
     *
     * <p>Dead-node detection: a node is excluded from the output if its full-precision vector
     * is absent from {@code postingsMap} (all its rows were deleted).
     *
     * <p>The TERMS_DATA component is written with a proper SAI codec header and footer: the
     * compacted jvector graph is first written to a temp file, then streamed through an
     * {@link org.apache.cassandra.index.sai.disk.io.IndexOutputWriter} so the CRC accumulates
     * over all bytes, producing a footer that survives SAI scrub validation.
     *
     * <p>When sources have {@code FUSED_PQ}, the compactor retrains the PQ codebook. The
     * retrained codebook is read back from the compacted graph file and written to the Cassandra
     * PQ component so that query encoding at search time uses the same codebook as the in-graph
     * compressed neighbor scores.
     *
     * @param postingsMap     vector → output postings map built during the row-by-row compaction pass
     * @param maxSegmentRowId the largest segment-local row ID in the compaction output
     * @return component metadata describing the written segment
     * @throws IllegalStateException if any source lacks {@code INLINE_VECTORS} or sources have
     *                               inconsistent {@code FUSED_PQ} presence
     */
    public SegmentMetadata.ComponentMetadataMap merge(
            ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap,
            int maxSegmentRowId) throws IOException
    {
        return merge(postingsMap, maxSegmentRowId, ProgressLimiter.UNLIMITED);
    }

    /**
     * As {@link #merge(ChronicleMap, int)}, but installs a {@link ProgressLimiter} on the jvector
     * compactor so the host can observe per-phase progress and throttle the merge's write bandwidth.
     *
     * @param progressLimiter the control surface to install; pass {@link ProgressLimiter#UNLIMITED}
     *                        for none (the behaviour of the two-argument overload)
     */
    public SegmentMetadata.ComponentMetadataMap merge(
            ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap,
            int maxSegmentRowId,
            ProgressLimiter progressLimiter) throws IOException
    {
        // --- Validate source feature sets ---
        boolean hasFusedPQ = validateFeatureSets(sources.stream().map(SourceSegment::graph).collect(toList()));

        // --- Step 1: Open graph views and build per-source live bitsets + ordinal mappings ---
        // All sources have INLINE_VECTORS, so full-precision vector lookup is always available
        // for dead-node detection via postings-map containment.
        var views = new ArrayList<OnDiskGraphIndex.View>(sources.size());
        var liveNodes = new ArrayList<FixedBitSet>(sources.size());
        var remappers = new ArrayList<OrdinalMapper>(sources.size());

        // Parallel arrays: output global ordinal g → source index and local node ID.
        var globalSrcIdxList = new ArrayList<Integer>();
        var globalNodeIdList = new ArrayList<Integer>();

        for (int s = 0; s < sources.size(); s++)
        {
            var src = sources.get(s);
            var view = src.graph().getView();
            views.add(view);

            int idBound = src.graph().getIdUpperBound();
            var bs = new FixedBitSet(idBound);
            var oldToNew = new HashMap<Integer, Integer>();

            var nodeIt = src.graph().getNodes(0);
            while (nodeIt.hasNext())
            {
                int nid = nodeIt.nextInt();
                VectorFloat<?> vec = view.getVector(nid);
                if (postingsMap.containsKey(vec))
                {
                    int globalOrdinal = globalSrcIdxList.size();
                    bs.set(nid);
                    oldToNew.put(nid, globalOrdinal);
                    globalSrcIdxList.add(s);
                    globalNodeIdList.add(nid);
                }
            }

            liveNodes.add(bs);
            remappers.add(new OrdinalMapper.MapMapper(oldToNew));
        }

        int totalGlobalOrdinals = globalSrcIdxList.size();
        if (totalGlobalOrdinals == 0)
            throw new IllegalStateException("CompactionGraphMerger: no surviving nodes found across all source segments; all rows may have been deleted");

        int[] globalToSrcIdx = globalSrcIdxList.stream().mapToInt(Integer::intValue).toArray();
        int[] globalToNodeId = globalNodeIdList.stream().mapToInt(Integer::intValue).toArray();

        try  // outer try: closes source graph views
        {
            // --- Step 2: jvector writes the compacted graph body directly into the SAI TERMS_DATA
            // component, after a reserved SAI header; the footer CRC is then computed over the in-place
            // header+body. No temp file, one write of the (potentially large) graph.
            var termsComponent = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA);
            Path termsFile = termsComponent.file().toJavaIOFile().toPath();

            var compactor = JVectorVersionUtil.executionContext().newCompactor(
                    sources.stream().map(SourceSegment::graph).collect(toList()),
                    liveNodes,
                    remappers,
                    similarityFunction,
                    // The merge's batch work runs on the shared, Cassandra-bounded build pool
                    // (JVectorVersionUtil.compactionBuildPool()); allow up to that many batches in flight so
                    // a single merge can use the pool's parallelism while total jvector build threads stay
                    // within the managed budget.
                    JVectorVersionUtil.compactionBuildThreads());
            // Install the host control surface: forwards jvector's per-phase progress to the merge
            // operation and admits its write bandwidth against the shared compaction throughput budget.
            compactor.setProgressLimiter(progressLimiter);

            // Reserve the SAI header, then have jvector write its body directly after it (compact
            // preserves [0, startOffset)); finally wrap with a footer whose CRC covers header+body.
            try (var termsOutput = termsComponent.openOutput(true))
            {
                SAICodecUtils.writeHeader(termsOutput);
            }
            long termsOffset = SAICodecUtils.headerSize();

            long compactStart = System.nanoTime();
            compactor.compact(termsFile, termsOffset);
            long compactMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - compactStart);
            long termsLength = Files.size(termsFile) - termsOffset;

            // When FUSED_PQ is in use, the compactor retrains the PQ codebook and embeds it in the
            // output graph. Read it back (from the in-place body offset) so the Cassandra PQ component
            // holds the same codebook used for in-graph compressed neighbor scoring.
            ProductQuantization retrainedPQ = null;
            if (hasFusedPQ)
            {
                try (var rs = ReaderSupplierFactory.open(termsFile))
                {
                    var compactedGraph = OnDiskGraphIndex.load(rs, termsOffset);
                    retrainedPQ = ((FusedPQ) compactedGraph.getFeatures().get(FeatureId.FUSED_PQ)).getPQ();
                }
            }

            SAICodecUtils.writeFooterForExternalBody(termsFile, termsOffset + termsLength);
            logger.info("CompactionGraphMerger: TERMS_DATA written in place ({} source segments, {} surviving ordinals, {} bytes body at offset {}) in {}ms",
                        sources.size(), totalGlobalOrdinals, termsLength, termsOffset, compactMs);

            perIndexComponents.context().getIndexMetrics().ifPresent(m -> {
                m.vectorMergeCount.inc();
                m.vectorMergeMillis.update(compactMs);
                m.vectorMergeBytesWritten.update(termsLength);
                m.vectorMergeSurvivingOrdinals.update(totalGlobalOrdinals);
            });
            logger.debug("CompactionGraphMerger measurement: vectorMergeCount+=1, vectorMergeMillis={}, vectorMergeBytesWritten={}, vectorMergeSurvivingOrdinals={}",
                         compactMs, termsLength, totalGlobalOrdinals);

            // --- Step 3: Write postings and PQ ---
            try (var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
                 var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true))
            {
                SAICodecUtils.writeHeader(postingsOutput);
                SAICodecUtils.writeHeader(pqOutput);

                var firstDiskAnn = sources.get(0).diskAnn();
                long pqOffset = pqOutput.getFilePointer();
                var version = perIndexComponents.context().version();

                if (hasFusedPQ)
                {
                    CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                                       firstDiskAnn.isPqUnitVectors(),
                                                       CompressionType.PRODUCT_QUANTIZATION,
                                                       version);
                    retrainedPQ.write(pqOutput.asSequentialWriter(), version.onDiskFormat().jvectorFileFormatVersion());
                }
                else
                {
                    CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                                       false,
                                                       CompressionType.NONE,
                                                       version);
                }
                long pqLength = pqOutput.getFilePointer() - pqOffset;

                // Write postings using ZERO_OR_ONE_TO_MANY.
                // MergedSourceVectorValues maps each output global ordinal directly to its source
                // and local node ID. Every ordinal here has postings (dead nodes were excluded in
                // Step 1 via the postings-map containment check).
                long postingsOffset = postingsOutput.getFilePointer();
                var ordinalMapper = new OrdinalMapper.IdentityMapper(totalGlobalOrdinals - 1);
                var rp = new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                              totalGlobalOrdinals - 1,
                                              maxSegmentRowId,
                                              null, null,
                                              ordinalMapper);

                var vectorValues = new MergedSourceVectorValues(globalToSrcIdx, globalToNodeId, dimension, views);
                if (V5OnDiskFormat.writeV5VectorPostings(version))
                {
                    new V5VectorPostingsWriter<Integer>(rp)
                            .writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap);
                }
                // else: V2 format doesn't support ZERO_OR_ONE_TO_MANY — fall back is handled at
                // the builder-selection level (merge path is only used for V5+)
                long postingsLength = postingsOutput.getFilePointer() - postingsOffset;

                SAICodecUtils.writeFooter(pqOutput);
                SAICodecUtils.writeFooter(postingsOutput);

                return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength,
                                                              postingsOffset, postingsLength,
                                                              pqOffset, pqLength);
            }
        }
        finally
        {
            for (var view : views)
            {
                try { view.close(); }
                catch (Exception e) { logger.warn("Error closing source graph view", e); }
            }
        }
    }

    /**
     * Validates that all source graphs have compatible feature sets and returns whether
     * {@code FUSED_PQ} is present. Extracted for testability — callers can pass
     * {@code OnDiskGraphIndex} instances directly without Cassandra infrastructure.
     *
     * @throws IllegalStateException if any graph lacks {@code INLINE_VECTORS}, or if
     *                               {@code FUSED_PQ} presence differs across sources
     */
    static boolean validateFeatureSets(List<OnDiskGraphIndex> graphs)
    {
        boolean hasFusedPQ = graphs.get(0).getFeatureSet().contains(FeatureId.FUSED_PQ);
        for (var graph : graphs)
        {
            var features = graph.getFeatureSet();
            if (!features.contains(FeatureId.INLINE_VECTORS))
                throw new IllegalStateException("CompactionGraphMerger requires INLINE_VECTORS; got " + features);
            if (features.contains(FeatureId.FUSED_PQ) != hasFusedPQ)
                throw new IllegalStateException("CompactionGraphMerger requires consistent FUSED_PQ presence across all sources");
        }
        return hasFusedPQ;
    }

    /**
     * Builds a {@link FixedBitSet} marking all jvector-live nodes in {@code graph} (i.e. all
     * non-tombstoned slots), without consulting the postings map.
     *
     * <p>When the on-disk graph has no tombstoned slots ({@code idUpperBound == size(0)}), all
     * node IDs fall in [0, liveCount) and a simple bulk-set suffices.  When tombstoned slots
     * exist ({@code idUpperBound > size(0)}), some stored node IDs equal or exceed
     * {@code size(0)}, so the bitset must be sized to {@code idUpperBound} and populated by
     * iterating the actual live-node IDs returned by {@link OnDiskGraphIndex#getNodes(int)}.
     *
     * <p>Used by tests to verify the tombstone-gap fix in isolation. Production code inlines
     * equivalent logic inside {@link #merge} with the additional postings-map check.
     */
    static FixedBitSet buildLiveBitset(OnDiskGraphIndex graph)
    {
        int liveCount = graph.size(0);
        int idBound = graph.getIdUpperBound();
        var bs = new FixedBitSet(idBound);
        if (idBound == liveCount)
        {
            bs.set(0, liveCount);
        }
        else
        {
            var nodeIt = graph.getNodes(0);
            while (nodeIt.hasNext()) bs.set(nodeIt.nextInt());
        }
        return bs;
    }

    /**
     * Builds a {@link FixedBitSet} marking nodes that are both jvector-live (non-tombstoned) and
     * Cassandra-alive (their vector appears as a key in {@code postingsMap}).
     *
     * <p>The bitset is sized to {@code idUpperBound} so that neighbor-ID lookups inside the
     * compactor never exceed the bitset bounds. Only valid for graphs with full-precision inline
     * vectors.
     */
    static FixedBitSet buildLiveBitset(OnDiskGraphIndex graph,
                                        OnDiskGraphIndex.View view,
                                        Map<VectorFloat<?>, ?> postingsMap)
    {
        int idBound = graph.getIdUpperBound();
        var bs = new FixedBitSet(idBound);
        var nodeIt = graph.getNodes(0);
        while (nodeIt.hasNext())
        {
            int nid = nodeIt.nextInt();
            VectorFloat<?> vec = view.getVector(nid);
            if (postingsMap.containsKey(vec))
                bs.set(nid);
        }
        return bs;
    }

    /**
     * A {@link RandomAccessVectorValues} backed by the merged source graphs. Each output global
     * ordinal {@code g} maps directly to a (source, localNodeId) pair via parallel arrays built
     * during dead-node detection, so lookup is O(1) with no binary search.
     *
     * <p>Not thread-safe: each source {@link OnDiskGraphIndex.View} has a single reader.
     */
    static class MergedSourceVectorValues implements RandomAccessVectorValues
    {
        private final List<OnDiskGraphIndex.View> views;
        private final int[] globalToSrcIdx;
        private final int[] globalToNodeId;
        private final int dimension;

        /**
         * Production constructor: caller supplies the parallel arrays built during Step 1 of
         * {@link CompactionGraphMerger#merge}.
         */
        MergedSourceVectorValues(int[] globalToSrcIdx, int[] globalToNodeId,
                                  int dimension, List<OnDiskGraphIndex.View> views)
        {
            this.views = views;
            this.globalToSrcIdx = globalToSrcIdx;
            this.globalToNodeId = globalToNodeId;
            this.dimension = dimension;
        }

        /**
         * Test-friendly constructor: builds sequential (no-dead-node) mappings from per-source
         * sizes. Source {@code s} occupies global ordinals [offset_s, offset_s + sizes[s]).
         */
        MergedSourceVectorValues(int[] sizes, int dimension, List<OnDiskGraphIndex.View> views)
        {
            this.views = views;
            this.dimension = dimension;
            int total = 0;
            for (int n : sizes) total += n;
            this.globalToSrcIdx = new int[total];
            this.globalToNodeId = new int[total];
            int g = 0;
            for (int s = 0; s < sizes.length; s++)
                for (int n = 0; n < sizes[s]; n++) { globalToSrcIdx[g] = s; globalToNodeId[g] = n; g++; }
        }

        @Override
        public int size()
        {
            return globalToSrcIdx.length;
        }

        @Override
        public int dimension()
        {
            return dimension;
        }

        @Override
        public VectorFloat<?> getVector(int globalOrdinal)
        {
            return views.get(globalToSrcIdx[globalOrdinal]).getVector(globalToNodeId[globalOrdinal]);
        }

        @Override
        public void getVectorInto(int globalOrdinal, VectorFloat<?> vector, int offset)
        {
            views.get(globalToSrcIdx[globalOrdinal]).getVectorInto(globalToNodeId[globalOrdinal], vector, offset);
        }

        @Override
        public boolean isValueShared()
        {
            return false;
        }

        @Override
        public RandomAccessVectorValues copy()
        {
            throw new UnsupportedOperationException("MergedSourceVectorValues cannot be copied");
        }
    }
}
