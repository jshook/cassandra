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

import com.google.common.util.concurrent.RateLimiter;

import io.github.jbellis.jvector.util.work.ProgressLimiter;
import io.github.jbellis.jvector.util.work.WorkStage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;

/**
 * Cassandra's single {@link ProgressLimiter} for an on-disk vector graph merge. It melds the two
 * facets jvector's compactor calls:
 *
 * <ul>
 *   <li><b>Progress (up):</b> {@link #onProgress} forwards jvector's per-phase counters to the
 *       merge's {@link VectorMergeOperation}, so {@code nodetool compactionstats} and
 *       {@code system_views.sstable_tasks} advance <em>while</em> {@code compact()} runs (rather than
 *       jumping from 0% to 100%).</li>
 *   <li><b>Throttle (down):</b> {@link #acquire} admits jvector's write bandwidth against the
 *       <em>shared</em> compaction rate limiter — the same budget {@code compaction_throughput_mb_per_sec}
 *       and {@code nodetool setcompactionthroughput} control — so the merge's (often dominant)
 *       internal write participates in that budget, not just the SAI-side copy.</li>
 * </ul>
 *
 * <p>jvector invokes both methods on the orchestrating (compaction) thread — the caller of
 * {@code compact()} — while the merge's batch work runs on the shared build pool. So {@link #acquire}
 * simply blocks that thread on the rate limiter, exactly like ordinary compaction blocking on the same
 * limiter. Both methods also serve as the merge's <b>cancellation checkpoint</b>: they call
 * {@link org.apache.cassandra.db.compaction.TableOperation#throwIfStopRequested()}, so a DROP or compaction
 * interrupt stops the merge here. jvector drains its in-flight workers before {@code compact()} unwinds, so
 * cancellation is clean — no source read survives — and it surfaces as the standard
 * {@code CompactionInterruptedException}, which the compaction framework handles without error noise.
 */
public class CompactionProgressLimiter implements ProgressLimiter
{
    private final VectorMergeOperation operation;

    /**
     * @param operation the merge's registered {@link VectorMergeOperation} to feed progress into
     */
    public CompactionProgressLimiter(VectorMergeOperation operation)
    {
        this.operation = operation;
    }

    @Override
    public void onProgress(WorkStage stage, long completed, long total)
    {
        // Cancellation checkpoint at each phase boundary: a stopped merge (DROP/interrupt) throws here and
        // jvector drains its workers before compact() unwinds, so no source read survives the cancellation.
        operation.throwIfStopRequested();
        operation.report(completed, total);
    }

    @Override
    public Grant acquire(long bytes)
    {
        // Cancellation checkpoint: acquire is called frequently (per write batch), so a stopped merge
        // unwinds promptly here rather than only at phase boundaries.
        operation.throwIfStopRequested();
        // Disabled throttling (compaction_throughput_mb_per_sec == 0) sets the shared limiter's rate
        // to Double.MAX_VALUE; skip acquiring to avoid needless work, mirroring Cassandra's own
        // compactionRateLimiterAcquire guard. getRateLimiter() itself never returns null.
        if (bytes <= 0 || DatabaseDescriptor.getCompactionThroughputMbPerSec() <= 0)
            return Grant.NOOP;

        RateLimiter rateLimiter = CompactionManager.instance.getRateLimiter();
        long remaining = bytes;
        while (remaining > 0)
        {
            int chunk = (int) Math.min(remaining, Integer.MAX_VALUE);
            rateLimiter.acquire(chunk);
            remaining -= chunk;
        }
        // Rate-limiter model: the cost is paid here, nothing to release.
        return Grant.NOOP;
    }
}
