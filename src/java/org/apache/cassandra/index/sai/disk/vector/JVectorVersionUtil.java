/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;

public class JVectorVersionUtil
{
    private static final Logger logger = LoggerFactory.getLogger(JVectorVersionUtil.class);

    /**
     * Whether to fuse quantized vectors into the graph when writing indexes, assuming all other conditions are met.
     * Variables are volatile to allow for changing in unit tests. They are only accessed on flush, so their access
     * is infrequent.
     */
    public static volatile boolean ENABLE_FUSED = CassandraRelevantProperties.SAI_VECTOR_ENABLE_FUSED.getBoolean();
    public static volatile boolean ENABLE_NVQ = CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ.getBoolean();
    public static final int NUM_SUB_VECTORS = CassandraRelevantProperties.SAI_VECTOR_NVQ_NUM_SUB_VECTORS.getInt();

    /**
     * Decide whether we should write NVQ vectors to disk.
     * With NVQ, we use M * (7 + D / M) bytes, where D is the number of dimensions and M is the number of subvectors.
     * For FP vectors, we trivially use 4D bytes
     * @param dimension vector dimension for the index
     * @param version SAI on disk version, which internally determines the jvector version
     * @return true if NVQ should be used for the graph or false otherwise
     */
    public static boolean shouldWriteNVQ(int dimension, Version version)
    {
        return ENABLE_NVQ && versionSupportsNVQ(version) && NUM_SUB_VECTORS * (7 + dimension / NUM_SUB_VECTORS) < 4 * dimension;
    }

    public static boolean versionSupportsNVQ(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 4;
    }

    /**
     * Decide whether to attempt to write the quantized vectors as fused parts of the graph. Note that this method
     * does not take into account whether the graph has enough information to build a quantization, as that depends on
     * external factors.
     * @param version the SAI on disk format to use when writing to disk
     * @return true if conditions are met, false otherwise
     */
    public static boolean shouldWriteFused(Version version)
    {
        return ENABLE_FUSED && versionSupportsFused(version);
    }

    public static boolean versionSupportsFused(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 6;
    }

    /**
     * Logs the effective JVector/SAI vector index configuration at startup so operators can see what parameters
     * are in effect. Per-index construction parameters show the defaults; they can be overridden per index
     * via CQL index options (e.g. WITH OPTIONS = {'maximum_node_connections': '32'}).
     * Global feature flags are controlled by the listed system properties.
     */
    public static void logStartupConfig()
    {
        // neighborOverflow and alpha are not stored in IndexWriterConfig when unset;
        // the actual defaults used at build time are inline in CassandraOnHeapGraph / CompactionGraph.
        float flushNeighborhoodOverflow = 1.0f;
        float compactionNeighborhoodOverflow = 1.2f;
        float flushAlphaHighDim = 1.2f;
        float compactionAlpha = 1.2f;

        String compressionMode = ENABLE_NVQ
                                 ? String.format("NVQ (num_sub_vectors=%d, -D%s=%d)",
                                                 NUM_SUB_VECTORS,
                                                 CassandraRelevantProperties.SAI_VECTOR_NVQ_NUM_SUB_VECTORS.getKey(),
                                                 NUM_SUB_VECTORS)
                                 : "PQ (per source_model; NVQ disabled, enable via -D" +
                                   CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ.getKey() + "=true)";

        logger.info("JVector/SAI vector index configuration:" +
                    "\n  construction defaults (per-index overridable via CQL index options):" +
                    "\n    outDegree (maximum_node_connections * 2):  {}" +
                    "\n    efConstruction (construction_beam_width):   {}" +
                    "\n    neighborOverflow:                           {} (flush) / {} (compaction)" +
                    "\n    alpha:                                      {} (flush, dim>3) / {} (compaction)" +
                    "\n    enableHierarchy (enable_hierarchy):         {}" +
                    "\n    fusedGraph (-D{}):   {}" +
                    "\n    compression:                                {}" +
                    "\n  search:" +
                    "\n    usePruning (-D{}): {}" +
                    "\n    rerankK (topKOverquery): limit-dependent via source_model overquery; default model applies tapered ~2x for compressed, decaying to 1x at large limits" +
                    "\n    vectorCacheBytes (-D{}): {}" +
                    "\n    maxTopK (-D{}): {}" +
                    "\n  global:" +
                    "\n    graphCompactionMerge (-D{}): {}" +
                    "\n    parallelEncoding (-D{}): {}",
                    IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS * 2,
                    IndexWriterConfig.DEFAULT_CONSTRUCTION_BEAM_WIDTH,
                    flushNeighborhoodOverflow, compactionNeighborhoodOverflow,
                    flushAlphaHighDim, compactionAlpha,
                    IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY,
                    CassandraRelevantProperties.SAI_VECTOR_ENABLE_FUSED.getKey(), ENABLE_FUSED,
                    compressionMode,
                    "cassandra.sai.jvector.use_pruning_default",
                    V3OnDiskFormat.JVECTOR_USE_PRUNING_DEFAULT,
                    CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getKey(),
                    CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getLong(),
                    CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getKey(),
                    IndexWriterConfig.MAX_TOP_K,
                    CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getKey(),
                    CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean(),
                    CassandraRelevantProperties.SAI_ENCODE_AND_WRITE_VECTOR_GRAPH_IN_PARALLEL_ENABLED.getKey(),
                    CassandraRelevantProperties.SAI_ENCODE_AND_WRITE_VECTOR_GRAPH_IN_PARALLEL_ENABLED.getBoolean());
    }
}
