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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.FixedBitSet;
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
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.LowPriorityThreadFactory;
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
 * <p><b>MVP limitations:</b>
 * <ul>
 *   <li>All source nodes are treated as live (FixedBitSet all-true). Dead nodes from deleted rows
 *       have no postings in the output and are invisible to queries, but do consume graph space.
 *       Accurate dead-node tracking is a follow-up.
 *   <li>The PQ codebook written to the Cassandra PQ file is taken from the first source segment
 *       rather than the retrained codebook from the compactor. This is correct but sub-optimal.
 *   <li>The TERMS_DATA output file is written at offset 0 (no SAI codec header/footer) so that
 *       the compactor can write directly. SAI checksum validation will fail for this component
 *       during scrubbing; normal querying is unaffected.
 * </ul>
 */
public class CompactionGraphMerger
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraphMerger.class);

    /** Killswitch: set to false to fall back to the legacy graph-rebuild path without restarting. */
    public static volatile boolean ENABLED = CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean();

    private static final ForkJoinPool compactionFjp = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            new LowPriorityThreadFactory(),
            null,
            false);

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
     * Returns true if all source graphs expose full-precision inline vectors, which is required for
     * the postings-mapping step to match vectors back to output row IDs. NVQ graphs store quantized
     * vectors only and are not supported by this merger.
     */
    public static boolean sourcesHaveInlineVectors(List<SourceSegment> sources)
    {
        for (var src : sources)
        {
            var features = src.graph().getFeatureSet();
            if (!features.contains(FeatureId.INLINE_VECTORS) && !features.contains(FeatureId.SEPARATED_VECTORS))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all source graphs use Product Quantization compression.
     */
    public static boolean sourcesUsePQ(List<SourceSegment> sources)
    {
        for (var src : sources)
        {
            try
            {
                src.diskAnn().getPQ();
            }
            catch (AssertionError e)
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Merges the source graphs and writes all SAI index components for the output segment.
     *
     * @param postingsMap     vector → output postings map built during the row-by-row compaction pass
     * @param maxSegmentRowId the largest segment-local row ID in the compaction output
     * @return component metadata describing the written segment
     */
    public SegmentMetadata.ComponentMetadataMap merge(
            ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap,
            int maxSegmentRowId) throws IOException
    {
        // --- Step 1: Build all-live FixedBitSets and sequential OffsetMappers ---
        var liveNodes = new ArrayList<FixedBitSet>(sources.size() - 1);
        var remappers = new ArrayList<OrdinalMapper>(sources.size() - 1);
        int offset = 0;
        for (var src : sources)
        {
            int size = src.graph().size(0);
            var bs = new FixedBitSet(size);
            bs.set(0, size);
            liveNodes.add(bs);
            remappers.add(new OrdinalMapper.OffsetMapper(offset, size));
            offset += size;
        }
        int totalGlobalOrdinals = offset;

        // --- Step 2: Run the jvector compactor ---
        // Write directly to the TERMS_DATA file at offset 0 (no SAI codec header; see class-level
        // MVP note). termsOffset is therefore 0.
        var termsFile = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA).file();
        var outputPath = termsFile.toJavaIOFile().toPath();

        logger.info("CompactionGraphMerger: merging {} source segments ({} total ordinals) into {}",
                    sources.size(), totalGlobalOrdinals, outputPath);

        var compactor = new OnDiskGraphIndexCompactor(
                sources.stream().map(SourceSegment::graph).collect(toList()),
                liveNodes,
                remappers,
                similarityFunction,
                compactionFjp);
        compactor.compact(outputPath);

        long termsOffset = 0;
        long termsLength = termsFile.length();
        logger.info("CompactionGraphMerger: compacted graph written ({} bytes)", termsLength);

        // --- Step 3: Open views for vector lookups during postings write ---
        var views = new ArrayList<OnDiskGraphIndex.View>(sources.size());
        for (var src : sources)
            views.add(src.graph().getView());

        try (var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
             var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(pqOutput);

            // --- Step 4: Write PQ from the first source (pre-compaction codebook) ---
            var firstDiskAnn = sources.get(0).diskAnn();
            long pqOffset = pqOutput.getFilePointer();
            var version = perIndexComponents.context().version();
            CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                               firstDiskAnn.isPqUnitVectors(),
                                               VectorCompression.CompressionType.PRODUCT_QUANTIZATION,
                                               version);
            ProductQuantization pq = firstDiskAnn.getPQ();
            pq.write(pqOutput.asSequentialWriter(), version.onDiskFormat().jvectorFileFormatVersion());
            long pqLength = pqOutput.getFilePointer() - pqOffset;

            // --- Step 5: Write postings using ZERO_OR_ONE_TO_MANY with identity mapper ---
            // For each global ordinal i, MergedSourceVectorValues reads the vector from the
            // appropriate source graph; postingsMap lookup returns the output row IDs.
            long postingsOffset = postingsOutput.getFilePointer();
            var ordinalMapper = new OrdinalMapper.IdentityMapper(totalGlobalOrdinals - 1);
            var rp = new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                          totalGlobalOrdinals - 1,
                                          maxSegmentRowId,
                                          null, null,
                                          ordinalMapper);

            var vectorValues = new MergedSourceVectorValues(sources, views);
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
     * A {@link RandomAccessVectorValues} backed by the merged source graphs. Ordinal {@code i}
     * maps to the source graph whose offset range covers {@code i}; the vector is read from that
     * source at the corresponding local ordinal.
     *
     * <p>Not thread-safe: each source {@link OnDiskGraphIndex.View} has a single reader.
     */
    private static class MergedSourceVectorValues implements RandomAccessVectorValues
    {
        private final List<SourceSegment> sources;
        private final List<OnDiskGraphIndex.View> views;
        private final int[] globalOffsets;
        private final int totalSize;
        private final int dimension;

        MergedSourceVectorValues(List<SourceSegment> sources, List<OnDiskGraphIndex.View> views)
        {
            this.sources = sources;
            this.views = views;
            this.globalOffsets = new int[sources.size()];
            int off = 0;
            for (int i = 0; i < sources.size(); i++)
            {
                globalOffsets[i] = off;
                off += sources.get(i).graph().size(0);
            }
            this.totalSize = off;
            this.dimension = sources.get(0).graph().getDimension();
        }

        @Override
        public int size()
        {
            return totalSize;
        }

        @Override
        public int dimension()
        {
            return dimension;
        }

        @Override
        public VectorFloat<?> getVector(int globalOrdinal)
        {
            int s = findSource(globalOrdinal);
            return views.get(s).getVector(globalOrdinal - globalOffsets[s]);
        }

        @Override
        public void getVectorInto(int globalOrdinal, VectorFloat<?> vector, int offset)
        {
            int s = findSource(globalOrdinal);
            views.get(s).getVectorInto(globalOrdinal - globalOffsets[s], vector, offset);
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

        private int findSource(int globalOrdinal)
        {
            int lo = 0, hi = sources.size() - 1;
            while (lo < hi)
            {
                int mid = (lo + hi + 1) >>> 1;
                if (globalOffsets[mid] <= globalOrdinal)
                    lo = mid;
                else
                    hi = mid - 1;
            }
            return lo;
        }
    }
}
