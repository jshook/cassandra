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

import org.junit.Test;

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraDiskAnn;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the DROP-under-merge use-after-unmap crash. A jvector graph merge
 * ({@code OnDiskGraphIndexCompactor}) reads source vectors out of the source segments' mmap'd
 * files on pool workers. Before the fix, the merge borrowed each source's live
 * {@link OnDiskGraphIndex} without holding a reference, so a concurrent {@code DROP} / index teardown
 * could release the owning {@link SSTableIndex}, unmap the file, and fault the in-flight read (SIGSEGV).
 * The fix (see {@code SegmentBuilder.VectorMergeSegmentBuilder.flushInternal}) pins every source
 * {@link SSTableIndex} for the merge's full lifetime.
 * <p>
 * A real mid-compaction timing race can't be a JUnit assertion — the failure mode is a JVM SIGSEGV, not a
 * catchable exception, and landing a DROP inside {@code compact()}'s read window deterministically would
 * need a byteman pause (unavailable on JDK 24+). So this test asserts the <em>invariant</em> the fix
 * relies on, deterministically and against a real on-disk vector index: while the merge holds its
 * reference, a concurrent {@code release()} does <b>not</b> unmap the source, and its graph stays readable
 * — the exact off-heap read the compactor performs. Without the merge's pin, the release would drop the
 * refcount to zero, unmap the file, and that read would crash.
 */
public class VectorMergeTeardownTest extends VectorTester
{
    private static final int DIMENSION = 16;

    @Test
    public void mergePinKeepsSourceMappedThroughConcurrentRelease() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, val vector<float, %d>, PRIMARY KEY(pk))", DIMENSION));
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        for (int pk = 0; pk < 200; pk++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk, randomVectorBoxed(DIMENSION));
        flush(); // one on-disk vector-index segment — a merge source

        StorageAttachedIndex index = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);

        // Find a source SSTableIndex whose segment carries an on-disk vector graph (the searcher the merge
        // reads through in SSTableIndexWriter.collectMergeSources).
        SSTableIndex source = null;
        CassandraDiskAnn diskAnn = null;
        outer:
        for (SSTableIndex ssti : index.getIndexContext().getView())
        {
            for (Segment segment : ssti.getSegments())
            {
                if (segment.getIndexSearcher() instanceof V2VectorIndexSearcher)
                {
                    source = ssti;
                    diskAnn = ((V2VectorIndexSearcher) segment.getIndexSearcher()).graph;
                    break outer;
                }
            }
        }
        assertThat(source).as("a flushed vector index should expose an on-disk graph source segment").isNotNull();
        assertThat(source.isReleased()).isFalse();

        // Two references we own, so the system's baseline reference is untouched and teardown stays balanced:
        //   refMerge = the pin VectorMergeSegmentBuilder.flushInternal holds across the merge
        //   refOwner = stand-in for the reference a concurrent DROP / index teardown releases
        assertThat(source.reference()).as("acquire the merge pin").isTrue();  // refMerge
        assertThat(source.reference()).as("acquire the droppable owner ref").isTrue();  // refOwner
        try
        {
            // DROP / index teardown releases its reference while the merge is still reading.
            source.release(); // release refOwner

            // The merge's pin defers the unmap: the source is NOT released/closed underneath the merge.
            assertThat(source.isReleased())
                .as("a source referenced by the in-flight merge must not be unmapped by a concurrent DROP")
                .isFalse();

            // ...and its on-disk graph is still mapped: read a vector exactly as the compactor does
            // (OnDiskGraphIndex.View.getVector -> off-heap Unsafe read). Without the merge's pin the
            // release above would have unmapped this file and this read would SIGSEGV.
            OnDiskGraphIndex graph = diskAnn.getOnDiskGraph();
            try (var view = graph.getView())
            {
                var nodes = graph.getNodes(0);
                assertThat(nodes.hasNext()).isTrue();
                VectorFloat<?> vector = view.getVector(nodes.nextInt());
                assertThat(vector).isNotNull();
                assertThat(vector.length()).isEqualTo(DIMENSION);
            }
        }
        finally
        {
            source.release(); // release refMerge (merge complete)
        }

        // Our two references are balanced; the system's baseline reference remains, so the index is still
        // live and ordinary teardown will close it.
        assertThat(source.isReleased()).isFalse();
    }
}
