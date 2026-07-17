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

package org.apache.cassandra.db.compaction.unified;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.SharedCompactionObserver;
import org.apache.cassandra.db.compaction.SharedCompactionProgress;
import org.apache.cassandra.db.compaction.SharedTableOperation;
import org.apache.cassandra.db.compaction.TableOperationObserver;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class UnifiedCompactionTask extends CompactionTask
{
    private final ShardManager shardManager;
    private final Range<Token> operationRange;
    private final Set<SSTableReader> actuallyCompact;
    private final SharedCompactionProgress sharedProgress;
    private final SharedTableOperation sharedOperation;
    private final UnifiedCompactionStrategy.ShardingStats shardingStats;

    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 ShardManager shardManager,
                                 UnifiedCompactionStrategy.ShardingStats shardingStats)
    {
        this(cfs, strategy, txn, gcBefore, false, shardManager, shardingStats, null, null, null, null, null);
    }


    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 boolean keepOriginals,
                                 ShardManager shardManager,
                                 UnifiedCompactionStrategy.ShardingStats shardingStats,
                                 Range<Token> operationRange,
                                 Collection<SSTableReader> actuallyCompact,
                                 SharedCompactionProgress sharedProgress,
                                 SharedCompactionObserver sharedObserver,
                                 SharedTableOperation sharedOperation)
    {
        super(cfs,
              txn,
              // Set the total operation sizes early to use in shared progress tracking. This assumes that:
              // - there are no expired sstables in the compaction (UCS processes them separately)
              // - sstable exclusion for lack of space does not apply (shared progress is only use when an operation
              //   range applies, which disables this)
              sharedProgress != null ? getOperationTotals(actuallyCompact, operationRange) : null,
              gcBefore,
              keepOriginals,
              strategy,
              sharedObserver != null ? sharedObserver : strategy);
        this.shardManager = shardManager;
        this.shardingStats = shardingStats;

        if (operationRange != null)
            assert actuallyCompact != null : "Ranged tasks should use a set of sstables to compact";

        this.operationRange = operationRange;
        this.sharedProgress = sharedProgress;
        this.sharedOperation = sharedOperation;
        if (sharedProgress != null)
            sharedProgress.registerExpectedSubtask(totals.inputUncompressedSize, totals.inputDiskSize, totals.inputUncompressedSize);
        if (sharedObserver != null)
            sharedObserver.registerExpectedSubtask();
        if (sharedOperation != null)
            sharedOperation.registerExpectedSubtask();
        // To make sure actuallyCompact tracks any removals from txn.originals(), we intersect the given set with it.
        // This should not be entirely necessary (as shouldReduceScopeForSpace() is false for ranged tasks), but it
        // is cleaner to enforce inputSSTables()'s requirements.
        this.actuallyCompact = actuallyCompact != null ? Sets.intersection(ImmutableSet.copyOf(actuallyCompact),
                                                                           txn.originals())
                                                       : txn.originals();
    }

    /**
     * Guaranteed terminal for every subtask (called from {@code execute()}'s finally block and
     * from {@code rejected()}). If this subtask terminated WITHOUT ever starting its compaction
     * operation — its inputs were emptied by a concurrent truncate/drop so it early-returned
     * before registering, or it was rejected before execution — it never released its
     * {@link SharedTableOperation} slot via the normal {@code onOperationStart}/{@code closeOne}
     * path. Release it here so the shared operation's refcount can reach zero and deregister
     * from ActiveOperations; otherwise it lingers forever as a phantom "active" compaction in
     * {@code system_views.sstable_tasks} — which in turn hangs any {@code await_empty} poller
     * (e.g. an index-finalize step) waiting for that table's compactions to drain.
     *
     * A subtask that DID start released the slot in {@code close()}; {@code operationStarted}
     * guards against the double release.
     */
    @Override
    protected Throwable cleanup(Throwable err)
    {
        err = super.cleanup(err);
        if (sharedOperation != null && !operationStarted)
            err = org.apache.cassandra.utils.Throwables.perform(err, sharedOperation::abandonExpectedSubtask);
        return err;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        // In multi-task operations we need to expire many ranges in a source sstable for early open. Not doable yet.
        final boolean earlyOpenAllowed = operationRange == null;
        return new ShardedCompactionWriter(realm,
                                           directories,
                                           transaction,
                                           nonExpiredSSTables,
                                           shardingStats.uniqueKeyRatio,
                                           keepOriginals,
                                           earlyOpenAllowed,
                                           shardManager.boundaries(shardingStats.shardCountForDensity));
    }

    @Override
    protected Range<Token> tokenRange()
    {
        return operationRange;
    }

    @Override
    protected SharedCompactionProgress sharedProgress()
    {
        return sharedProgress;
    }

    @Override
    protected boolean shouldReduceScopeForSpace()
    {
        // Because parallelized tasks share input sstables, we can't reduce the scope of individual tasks
        // (as doing that will leave some part of an sstable out of the compaction but still drop the whole sstable
        // when the task set completes).
        return tokenRange() == null;
    }

    @Override
    public Set<SSTableReader> inputSSTables()
    {
        return actuallyCompact;
    }

    @Override
    public AbstractCompactionTask setOpObserver(TableOperationObserver opObserver)
    {
        if (sharedOperation != null)
            opObserver = sharedOperation.wrapObserver(opObserver);
        return super.setOpObserver(opObserver);
    }

    @Override
    public long getSpaceOverhead()
    {
        if (operationRange != null)
        {
            // totals must be precalculated for ranged tasks
            return (long) (totals.inputDiskSize * shardingStats.overheadToDataRatio);
        }
        else
        {
            // if we don't have a range, the sharding stats have precise total disk space
            return (long) (shardingStats.totalOnDiskSize * shardingStats.overheadToDataRatio);
        }
    }
}