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

package org.apache.cassandra.index.sai.virtual;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractMutableVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A writable virtual table ({@code system_views.jvector}) exposing the runtime-tunable jvector
 * vector-index compaction/insert parameters. Reads show current values; an {@code UPDATE ... SET value =
 * '...' WHERE name = '...'} routes, in-process, to the same {@link JVectorVersionUtil} setters the JMX
 * MBean delegates to — so the vtable path is not serialized behind the JMX server. Changes are in-memory
 * (not persisted), like a JMX set.
 */
public class JVectorSystemView extends AbstractMutableVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(JVectorSystemView.class);

    public static final String TABLE_NAME = "jvector";

    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String WRITABLE = "writable";
    private static final String EFFECTIVE_WHEN = "effective_when";

    // Parameter names (partition keys) — the recently-added jvector compaction/insert knobs.
    private static final String P_MERGE_ENABLED = "graph_compaction_merge_enabled";
    private static final String P_BYTES_PER_ORDINAL = "compaction_merge_bytes_per_ordinal";
    private static final String P_BUILD_THREADS = "compaction_build_threads";
    private static final String P_INSERT_INFLIGHT_MB = "compaction_insert_inflight_mb";
    private static final String P_AMORTIZE_PQ = "amortize_pq_encoding";
    private static final String P_SERIALIZE_FLUSH_PQ = "serialize_flush_pq";

    public JVectorSystemView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Runtime-tunable jvector vector-index compaction/insert parameters")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .addRegularColumn(WRITABLE, BooleanType.instance)
                           .addRegularColumn(EFFECTIVE_WHEN, UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        addRow(result, P_MERGE_ENABLED, Boolean.toString(JVectorVersionUtil.isGraphCompactionMergeEnabled()), "next segment build");
        addRow(result, P_BYTES_PER_ORDINAL, Integer.toString(JVectorVersionUtil.getMergeBytesPerOrdinal()), "next merge");
        addRow(result, P_BUILD_THREADS, Integer.toString(JVectorVersionUtil.getDesiredCompactionBuildThreads()), "next compaction (lazy pool rebuild)");
        addRow(result, P_INSERT_INFLIGHT_MB, Integer.toString(JVectorVersionUtil.getInsertInflightMb()), "next segment build");
        addRow(result, P_AMORTIZE_PQ, Boolean.toString(JVectorVersionUtil.isAmortizePqEncoding()), "next memtable");
        addRow(result, P_SERIALIZE_FLUSH_PQ, Boolean.toString(JVectorVersionUtil.isSerializeFlushPq()), "next flush");
        return result;
    }

    private static void addRow(SimpleDataSet result, String name, String value, String effectiveWhen)
    {
        result.row(name)
              .column(VALUE, value)
              .column(WRITABLE, Boolean.TRUE)
              .column(EFFECTIVE_WHEN, effectiveWhen);
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        String name = partitionKey.value(0);
        if (columnValue.isEmpty())
            return; // row marker with no cell — nothing to set

        ColumnValue cv = columnValue.get();
        if (!VALUE.equals(cv.name()))
            throw new InvalidRequestException(String.format("Only the '%s' column is writable on %s", VALUE, metadata()));

        String value = cv.value();
        applySet(name, value);
        logger.info("jvector parameter '{}' set to '{}' via system_views.{}", name, value, TABLE_NAME);
    }

    private void applySet(String name, String value)
    {
        switch (name)
        {
            case P_MERGE_ENABLED:
                JVectorVersionUtil.setGraphCompactionMergeEnabled(parseBool(name, value));
                break;
            case P_BYTES_PER_ORDINAL:
                JVectorVersionUtil.setMergeBytesPerOrdinal(parsePositiveInt(name, value));
                break;
            case P_BUILD_THREADS:
                JVectorVersionUtil.setCompactionBuildThreads(parseInt(name, value));
                break;
            case P_INSERT_INFLIGHT_MB:
                JVectorVersionUtil.setInsertInflightMb(parseInt(name, value));
                break;
            case P_AMORTIZE_PQ:
                JVectorVersionUtil.setAmortizePqEncoding(parseBool(name, value));
                break;
            case P_SERIALIZE_FLUSH_PQ:
                JVectorVersionUtil.setSerializeFlushPq(parseBool(name, value));
                break;
            default:
                throw new InvalidRequestException(String.format(
                    "Unknown or read-only jvector parameter '%s' on %s", name, metadata()));
        }
    }

    private boolean parseBool(String name, String value)
    {
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new InvalidRequestException(String.format("Parameter '%s' expects true/false, got '%s'", name, value));
    }

    private int parseInt(String name, String value)
    {
        try
        {
            return Integer.parseInt(value.trim());
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("Parameter '%s' expects an integer, got '%s'", name, value));
        }
    }

    private int parsePositiveInt(String name, String value)
    {
        int i = parseInt(name, value);
        if (i < 1)
            throw new InvalidRequestException(String.format("Parameter '%s' expects a positive integer, got '%s'", name, value));
        return i;
    }
}
