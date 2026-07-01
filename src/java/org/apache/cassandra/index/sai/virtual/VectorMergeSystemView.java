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
package org.apache.cassandra.index.sai.virtual;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} exposing the per-index on-disk vector graph merge (jvector compaction)
 * measurements recorded in {@link org.apache.cassandra.index.sai.metrics.IndexMetrics}: the merge
 * count and the latency/bytes-written/surviving-ordinal distributions. Only vector indexes are
 * listed. The same values are exported through the metrics registry; this view makes them
 * queryable via CQL without a metrics scrape.
 */
public class VectorMergeSystemView extends AbstractVirtualTable
{
    static final String NAME = "vector_merges";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String INDEX_NAME = "index_name";
    static final String TABLE_NAME = "table_name";
    static final String MERGE_COUNT = "merge_count";
    static final String MERGE_MILLIS_MEAN = "merge_millis_mean";
    static final String MERGE_MILLIS_MAX = "merge_millis_max";
    static final String BYTES_WRITTEN_MEAN = "bytes_written_mean";
    static final String BYTES_WRITTEN_MAX = "bytes_written_max";
    static final String SURVIVING_ORDINALS_MEAN = "surviving_ordinals_mean";
    static final String SURVIVING_ORDINALS_MAX = "surviving_ordinals_max";

    public VectorMergeSystemView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .comment("On-disk vector graph merge (jvector compaction) measurements per index")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(INDEX_NAME, UTF8Type.instance)
                           .addRegularColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(MERGE_COUNT, LongType.instance)
                           .addRegularColumn(MERGE_MILLIS_MEAN, DoubleType.instance)
                           .addRegularColumn(MERGE_MILLIS_MAX, LongType.instance)
                           .addRegularColumn(BYTES_WRITTEN_MEAN, DoubleType.instance)
                           .addRegularColumn(BYTES_WRITTEN_MAX, LongType.instance)
                           .addRegularColumn(SURVIVING_ORDINALS_MEAN, DoubleType.instance)
                           .addRegularColumn(SURVIVING_ORDINALS_MAX, LongType.instance)
                           .build());
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        for (String ks : Schema.instance.getUserKeyspaces().names())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
            if (keyspace == null)
                throw new IllegalArgumentException("Unknown keyspace " + ks);

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
                if (group == null)
                    continue;

                for (Index index : group.getIndexes())
                {
                    IndexContext context = ((StorageAttachedIndex) index).getIndexContext();
                    if (!context.isVector() || context.getIndexMetrics().isEmpty())
                        continue;

                    var metrics = context.getIndexMetrics().get();
                    var millis = metrics.vectorMergeMillis.getSnapshot();
                    var bytes = metrics.vectorMergeBytesWritten.getSnapshot();
                    var ordinals = metrics.vectorMergeSurvivingOrdinals.getSnapshot();

                    dataset.row(ks, context.getIndexName())
                           .column(TABLE_NAME, cfs.name)
                           .column(MERGE_COUNT, metrics.vectorMergeCount.getCount())
                           .column(MERGE_MILLIS_MEAN, millis.getMean())
                           .column(MERGE_MILLIS_MAX, millis.getMax())
                           .column(BYTES_WRITTEN_MEAN, bytes.getMean())
                           .column(BYTES_WRITTEN_MAX, bytes.getMax())
                           .column(SURVIVING_ORDINALS_MEAN, ordinals.getMean())
                           .column(SURVIVING_ORDINALS_MAX, ordinals.getMax());
                }
            }
        }

        return dataset;
    }
}
