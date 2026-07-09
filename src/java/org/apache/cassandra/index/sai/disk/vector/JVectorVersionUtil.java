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

import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.EmbeddedExecutionContext;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.utils.BoundedFanout;
import org.apache.cassandra.index.sai.utils.InsertFanout;
import org.apache.cassandra.index.sai.utils.LowPriorityThreadFactory;
import org.apache.cassandra.index.sai.utils.ResizableSemaphore;
import org.apache.cassandra.index.sai.utils.UnboundedFanout;

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
     * A lazily-rebuilt bundle of the shared jvector build/compaction {@link ForkJoinPool} and the
     * {@link EmbeddedExecutionContext} wired to it, at a fixed worker-thread count.
     */
    private static final class BuildPool
    {
        final int threads;
        final ForkJoinPool pool;
        final EmbeddedExecutionContext context;

        BuildPool(int threads)
        {
            this.threads = Math.max(1, threads);
            // A ForkJoinPool is required: jvector's submit-then-join build pattern is only safe (no
            // thread-starvation deadlock) on a work-stealing pool, and product quantization needs one.
            this.pool = new ForkJoinPool(this.threads, new LowPriorityThreadFactory(), null, false);
            this.context = EmbeddedExecutionContext.of(pool);
        }
    }

    /**
     * The desired worker-thread count for the shared jvector build/compaction pool. Settable at runtime
     * via {@link #setCompactionBuildThreads(int)}; a change takes effect by lazily rebuilding the pool
     * before the next compaction that requests the context (in-flight compactions are undisturbed). A
     * value &lt;= 0 from {@link CassandraRelevantProperties#SAI_VECTOR_COMPACTION_BUILD_THREADS} derives
     * from {@code concurrent_compactors}.
     */
    private static volatile int desiredBuildThreads = resolveCompactionBuildThreads();

    /**
     * The current build pool + context: a single node-wide, low-priority {@link ForkJoinPool} that bounds
     * all jvector graph construction, cleanup, PQ/NVQ, merge, and re-encode parallelism to a
     * Cassandra-managed budget rather than the physical core count, shared by flush, rebuild, and merge.
     */
    private static volatile BuildPool buildPool = new BuildPool(desiredBuildThreads);
    private static final Object buildPoolLock = new Object();

    /**
     * Returns the current build pool, first lazily replacing it (before the caller's compaction proceeds)
     * if {@link #desiredBuildThreads} changed since it was built. A {@code ForkJoinPool}'s parallelism
     * cannot change in place, so a size change installs a fresh pool for subsequent compactions; in-flight
     * compactions keep the pool they already captured, and the superseded pool's idle worker threads time
     * out (keepalive) and it is reclaimed once no compaction references it.
     */
    private static BuildPool currentBuildPool()
    {
        BuildPool p = buildPool;
        if (p.threads == desiredBuildThreads)
            return p;
        synchronized (buildPoolLock)
        {
            p = buildPool;
            if (p.threads != desiredBuildThreads)
                buildPool = p = new BuildPool(desiredBuildThreads);
            return p;
        }
    }

    /** The shared jvector build/compaction execution context (rebuilt lazily on a thread-count change). */
    public static EmbeddedExecutionContext executionContext()
    {
        return currentBuildPool().context;
    }

    /** The shared jvector build/compaction {@link ForkJoinPool} backing {@link #executionContext()}. */
    public static ForkJoinPool compactionBuildPool()
    {
        return currentBuildPool().pool;
    }

    /** The current worker-thread count of the shared build/compaction pool. */
    public static int compactionBuildThreads()
    {
        return currentBuildPool().threads;
    }

    /** The desired worker-thread count a running compaction will pick up on its next context request. */
    public static int getDesiredCompactionBuildThreads()
    {
        return desiredBuildThreads;
    }

    /**
     * Set the desired worker-thread count for the shared build/compaction pool. Takes effect by lazily
     * replacing the pool before the next compaction that requests the context; in-flight compactions are
     * undisturbed. A value &lt;= 0 derives from {@code concurrent_compactors}. Not persisted (in-memory,
     * like a JMX set) — the configured property remains the startup default.
     */
    public static void setCompactionBuildThreads(int threads)
    {
        int resolved = threads > 0 ? threads : DatabaseDescriptor.getConcurrentCompactors();
        desiredBuildThreads = Math.max(1, resolved);
    }

    private static int resolveCompactionBuildThreads()
    {
        int configured = CassandraRelevantProperties.SAI_VECTOR_COMPACTION_BUILD_THREADS.getInt();
        int threads = configured > 0 ? configured : DatabaseDescriptor.getConcurrentCompactors();
        return Math.max(1, threads);
    }

    /**
     * Desired node-wide in-flight insert budget in permits (bytes, capped at ~2 GiB), or {@code 0} to
     * disable the bound (unbounded fan-out). Settable at runtime via {@link #setInsertInflightMb(int)};
     * {@link #newInsertFanout()} picks the policy and resizes the shared budget on the next segment build.
     * See {@link CassandraRelevantProperties#SAI_VECTOR_COMPACTION_INSERT_INFLIGHT_MB}.
     */
    private static volatile int desiredInflightPermits = resolveInsertInflightPermits();

    /**
     * The shared node-wide insert-admission gate, resized in place when the budget changes (affecting all
     * bounded fan-outs that share it). Created with the startup budget; in unbounded mode it holds 0
     * permits and is simply unused.
     */
    private static final ResizableSemaphore INSERT_BUDGET = new ResizableSemaphore(Math.max(0, desiredInflightPermits));

    /**
     * A per-build {@link InsertFanout} over the current build pool. Thread concurrency is bounded by the
     * pool; the admission policy is chosen live from {@link #desiredInflightPermits} — a
     * {@link BoundedFanout} gated by the shared (in-place-resized) budget, or an {@link UnboundedFanout}
     * when the budget is disabled.
     */
    public static InsertFanout newInsertFanout()
    {
        ForkJoinPool pool = currentBuildPool().pool;
        int permits = desiredInflightPermits;
        if (permits <= 0)
            return new UnboundedFanout(pool);
        if (INSERT_BUDGET.totalPermits() != permits)
            INSERT_BUDGET.setTotalPermits(permits);
        return new BoundedFanout(pool, INSERT_BUDGET, permits);
    }

    /** The current in-flight insert budget in MiB, or 0 if unbounded. */
    public static int getInsertInflightMb()
    {
        int permits = desiredInflightPermits;
        return permits <= 0 ? 0 : permits / (1024 * 1024);
    }

    /**
     * Set the node-wide in-flight insert budget in MiB, or 0 to disable the bound (unbounded fan-out).
     * Takes effect on the next segment build; the shared budget is resized in place. Not persisted.
     */
    public static void setInsertInflightMb(int mb)
    {
        desiredInflightPermits = mb <= 0 ? 0 : (int) Math.min(Integer.MAX_VALUE, (long) mb * 1024L * 1024L);
    }

    private static int resolveInsertInflightPermits()
    {
        int mb = CassandraRelevantProperties.SAI_VECTOR_COMPACTION_INSERT_INFLIGHT_MB.getInt();
        if (mb <= 0)
            return 0; // disabled: unbounded fan-out
        return (int) Math.min(Integer.MAX_VALUE, (long) mb * 1024L * 1024L);
    }

    /**
     * Live per-surviving-ordinal on-heap memory estimate (bytes) charged for a vector graph merge. Read
     * fresh on each merge, so a change takes effect immediately.
     */
    private static volatile int mergeBytesPerOrdinal =
        CassandraRelevantProperties.SAI_VECTOR_COMPACTION_MERGE_BYTES_PER_ORDINAL.getInt();

    /** The per-surviving-ordinal memory estimate (bytes) charged for a vector graph merge. */
    public static int getMergeBytesPerOrdinal()
    {
        return mergeBytesPerOrdinal;
    }

    /** Set the per-surviving-ordinal merge memory estimate (bytes); takes effect on the next merge. */
    public static void setMergeBytesPerOrdinal(int bytes)
    {
        mergeBytesPerOrdinal = Math.max(1, bytes);
    }

    /**
     * Whether a memtable vector graph encodes PQ codes incrementally during ingest (adopting an existing
     * on-disk codebook) so its flush serializes pre-built codes instead of encoding every vector in one
     * flush-time burst. Read when a memtable graph is created, so a change is effective for the next new
     * memtable. See {@link CassandraRelevantProperties#SAI_VECTOR_AMORTIZE_PQ_ENCODING}.
     */
    private static volatile boolean amortizePqEncoding =
        CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.getBoolean();

    /** Whether memtable flushes encode PQ incrementally during ingest (vs. one flush-time burst). */
    public static boolean isAmortizePqEncoding()
    {
        return amortizePqEncoding;
    }

    /** Enable/disable incremental PQ encoding during ingest; takes effect for the next new memtable. */
    public static void setAmortizePqEncoding(boolean enabled)
    {
        amortizePqEncoding = enabled;
    }

    /**
     * Whether any residual flush-time PQ compute/encode (the cold-start or non-amortized fallback) is
     * serialized node-wide on a process-wide lock, restoring the pre-removal behavior. Off by default, so
     * concurrent flushes parallelize like core sstable flushing. Read at flush (effective next flush).
     * See {@link CassandraRelevantProperties#SAI_VECTOR_SERIALIZE_FLUSH_PQ}.
     */
    private static volatile boolean serializeFlushPq =
        CassandraRelevantProperties.SAI_VECTOR_SERIALIZE_FLUSH_PQ.getBoolean();

    /** Whether residual flush-time PQ work is serialized node-wide (default false). */
    public static boolean isSerializeFlushPq()
    {
        return serializeFlushPq;
    }

    /** Enable/disable node-wide serialization of residual flush-time PQ work; takes effect next flush. */
    public static void setSerializeFlushPq(boolean enabled)
    {
        serializeFlushPq = enabled;
    }

    /** Whether vector-index compaction merges existing on-disk graphs (vs. the legacy rebuild path). */
    public static boolean isGraphCompactionMergeEnabled()
    {
        return CompactionGraphMerger.ENABLED;
    }

    /** Enable/disable the vector graph-compaction merge path; takes effect on the next segment build. */
    public static void setGraphCompactionMergeEnabled(boolean enabled)
    {
        CompactionGraphMerger.ENABLED = enabled;
    }

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

        logger.info("JVector graph build/compaction pool: {} worker threads (-D{}={}, 0 => concurrent_compactors={})",
                    compactionBuildThreads(),
                    CassandraRelevantProperties.SAI_VECTOR_COMPACTION_BUILD_THREADS.getKey(),
                    CassandraRelevantProperties.SAI_VECTOR_COMPACTION_BUILD_THREADS.getInt(),
                    DatabaseDescriptor.getConcurrentCompactors());
        logger.info("JVector insert fan-out: {} (-D{})",
                    desiredInflightPermits > 0
                        ? (getInsertInflightMb() + " MiB in-flight budget (bounded)")
                        : "unbounded (in-flight budget disabled)",
                    CassandraRelevantProperties.SAI_VECTOR_COMPACTION_INSERT_INFLIGHT_MB.getKey());
        logger.info("JVector memtable flush PQ: amortized encoding {} (-D{}), residual flush-time PQ {} (-D{})",
                    amortizePqEncoding ? "on (encode during ingest)" : "off (encode at flush)",
                    CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.getKey(),
                    serializeFlushPq ? "serialized node-wide" : "parallel",
                    CassandraRelevantProperties.SAI_VECTOR_SERIALIZE_FLUSH_PQ.getKey());
    }
}
