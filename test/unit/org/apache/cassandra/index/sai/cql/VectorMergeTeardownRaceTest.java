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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraDiskAnn;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Byteman-gated "true repro" companion to {@link VectorMergeTeardownTest}. Where that test asserts the
 * ref-count <em>invariant</em> against synthetic references, this one drives a <b>real</b> jvector graph
 * merge under a live compaction, deterministically parks it mid-flight, and verifies the wiring the
 * invariant test cannot reach: that {@code SegmentBuilder.VectorMergeSegmentBuilder.flushInternal} actually
 * pins every source {@link SSTableIndex} for the merge's lifetime.
 * <p>
 * A byteman barrier parks the compaction thread at the entry of {@link CompactionGraphMerger}{@code .merge}.
 * By that point {@code flushInternal} has already referenced each source (the pin is acquired before
 * {@code merge()} is called), so while parked the test proves, via read-only reflection on the source's
 * reference count, that each source carries the merge's extra reference. It then reads a vector out of a
 * pinned source's on-disk graph — the exact off-heap {@code OnDiskGraphIndex.View.getVector} read the
 * compactor performs on its pool workers — which would SIGSEGV if a concurrent {@code DROP} had been allowed
 * to unmap the file. Finally it releases the merge and confirms it completes cleanly and the merged index
 * still answers ANN queries. Remove the pin and the mid-merge assertion fails cleanly (the merge no longer
 * adds a reference); on the real node, the same missing pin is the use-after-unmap crash.
 * <p>
 * The whole test is gated to <b>JDK &lt; 24</b>: the {@link Injections} framework attaches byteman as a
 * dynamic agent, which the JDK forbids from 24 onward.
 */
public class VectorMergeTeardownRaceTest extends VectorTester
{
    private static final int DIMENSION = 16;
    private static final long MERGE_START_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    @Test
    public void concurrentReleaseDuringLiveMergeKeepsSourcesPinned() throws Throwable
    {
        // The Injections framework attaches byteman as a dynamic agent, which is rejected on JDK 24+.
        assumeTrue("byteman dynamic agent-attach is unavailable on JDK 24+", Runtime.version().feature() < 24);

        // Force the streaming-merge path rather than a rebuild: V5+ vector postings (onOrAfter DC) with
        // INLINE_VECTORS on every source (NVQ off). See SSTableIndexWriter.newSegmentBuilder.
        SAIUtil.setCurrentVersion(Version.LATEST);
        SAIUtil.setEnableNVQ(false);

        createTable(String.format("CREATE TABLE %%s (pk int, val vector<float, %d>, PRIMARY KEY(pk))", DIMENSION));
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction();

        // >= 2 sstables, each with a PQ-bearing INLINE_VECTORS vector segment, so the compaction takes the
        // merge path (>= 2 mergeable sources required).
        int rowsPerFlush = Math.max(CassandraOnHeapGraph.MIN_PQ_ROWS, 256);
        int pk = 0;
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < rowsPerFlush; i++)
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, randomVectorBoxed(DIMENSION));
            flush();
        }

        StorageAttachedIndex index = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);

        // Capture the pre-compaction source SSTableIndex objects (the ones the merge will pin) and their
        // baseline reference counts, plus one source's on-disk graph to read through while the merge is parked.
        List<SSTableIndex> sources = new ArrayList<>();
        Map<SSTableIndex, Integer> baseline = new HashMap<>();
        CassandraDiskAnn probeGraph = null;
        for (SSTableIndex ssti : index.getIndexContext().getView())
        {
            for (Segment segment : ssti.getSegments())
            {
                if (segment.getIndexSearcher() instanceof V2VectorIndexSearcher)
                {
                    sources.add(ssti);
                    baseline.put(ssti, referenceCount(ssti));
                    if (probeGraph == null)
                        probeGraph = ((V2VectorIndexSearcher) segment.getIndexSearcher()).graph;
                    break;
                }
            }
        }
        assertThat(sources.size()).as("expected >= 2 vector-index sources to merge").isGreaterThanOrEqualTo(2);

        // Park the real merge at its entry. VectorMergeSegmentBuilder.flushInternal references (pins) every
        // source before it calls merge(), so at this injection point the pin is already held.
        Injections.Barrier mergeParked = Injections.newBarrier("vector_merge_parked", 2, false)
                                                    .add(InvokePointBuilder.newInvokePoint()
                                                                           .onClass(CompactionGraphMerger.class)
                                                                           .onMethod("merge"))
                                                    .build();
        Injections.inject(mergeParked);

        ExecutorService compactor = Executors.newSingleThreadExecutor();
        Future<?> compaction = compactor.submit((Callable<Void>) () -> { compact(); return null; });
        try
        {
            // Wait for the merge to reach (and park at) its entry. If the compaction finishes without the
            // barrier ever firing, the merge path was not taken and the test cannot make its point.
            long deadline = System.nanoTime() + MERGE_START_TIMEOUT_NANOS;
            while (mergeParked.getCount() > 1)
            {
                if (compaction.isDone())
                {
                    compaction.get(); // surface any compaction failure
                    fail("compaction finished without taking the vector graph merge path");
                }
                if (System.nanoTime() > deadline)
                    fail("timed out waiting for the vector graph merge to start");
                Thread.sleep(20);
            }

            // The merge is parked mid-flight with its pin held. Prove each source now carries the merge's
            // extra reference (baseline + the pin). Remove the pin in flushInternal and this fails cleanly.
            for (SSTableIndex source : sources)
            {
                assertThat(referenceCount(source))
                    .as("the in-flight merge must pin source %s (baseline reference count %d)", source, baseline.get(source))
                    .isGreaterThan(baseline.get(source));
            }

            // Read a vector out of a pinned source's on-disk graph exactly as the compactor does
            // (OnDiskGraphIndex.View.getVector -> off-heap Unsafe read). This is the read that faults if a
            // concurrent DROP is allowed to unmap the file; the merge's pin is what keeps it mapped.
            OnDiskGraphIndex graph = probeGraph.getOnDiskGraph();
            try (var view = graph.getView())
            {
                var nodes = graph.getNodes(0);
                assertThat(nodes.hasNext()).isTrue();
                VectorFloat<?> vector = view.getVector(nodes.nextInt());
                assertThat(vector).isNotNull();
                assertThat(vector.length()).isEqualTo(DIMENSION);
            }

            // Release the merge and let it run to completion.
            mergeParked.countDown();
            compaction.get();
        }
        finally
        {
            // If we bailed before releasing the barrier, interrupt the parked merge so nothing hangs; the
            // byteman await is interruptible.
            compactor.shutdownNow();
            mergeParked.disable();
            Injections.deleteAll();
        }

        // The merge completed and released its pin; the compaction then obsoletes the merged-away source
        // sstables. Both references gone, every source must end fully released. A leaked pin would instead
        // leave a source stuck referenced and its file never unmapped.
        for (SSTableIndex source : sources)
        {
            assertThat(source.isReleased())
                .as("merge must release its pin so the compacted-away source %s is fully released (no leak)", source)
                .isTrue();
        }

        // ...and the merged index still answers ANN queries.
        assertRowCount(execute("SELECT * FROM %s ORDER BY val ANN OF ? LIMIT 10", randomVectorBoxed(DIMENSION)), 10);
    }

    /**
     * Reads {@link SSTableIndex}'s private reference counter without mutating it. Observing the exact count
     * lets the test prove the merge added its pin without adding a competing reference of its own (which
     * would mask the very unmap the pin prevents).
     */
    private static int referenceCount(SSTableIndex index) throws ReflectiveOperationException
    {
        Field field = SSTableIndex.class.getDeclaredField("references");
        field.setAccessible(true);
        return ((AtomicInteger) field.get(index)).get();
    }
}
