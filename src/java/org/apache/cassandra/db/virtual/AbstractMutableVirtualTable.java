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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A writable {@link AbstractVirtualTable} that decomposes an incoming {@link PartitionUpdate} into typed
 * mutation hooks, so subclasses handle updates/deletions at the level of decoded column values rather than
 * raw byte buffers. Ported from Apache Cassandra (CASSANDRA-15254) and adapted to this tree's APIs.
 *
 * <p>Subclasses override only the hooks they support (each unsupported hook throws
 * {@link InvalidRequestException} by default). The most common is {@link #applyColumnUpdate} for
 * {@code UPDATE ... SET col = value WHERE ...} / {@code INSERT}.
 */
public abstract class AbstractMutableVirtualTable extends AbstractVirtualTable
{
    protected AbstractMutableVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @Override
    public final void apply(PartitionUpdate update)
    {
        ColumnValues partitionKey = ColumnValues.from(metadata(), update.partitionKey());

        if (update.deletionInfo().isLive())
        {
            for (Row row : update.rows())
            {
                ColumnValues clusteringColumns = ColumnValues.from(metadata(), row.clustering());

                if (row.deletion().isLive())
                {
                    if (row.columnCount() == 0)
                    {
                        applyColumnUpdate(partitionKey, clusteringColumns, Optional.empty());
                    }
                    else
                    {
                        for (ColumnData columnData : row)
                        {
                            ColumnMetadata column = columnData.column();
                            if (column.isComplex())
                                throw new InvalidRequestException(
                                    String.format("Complex type columns are not supported by table %s", metadata()));

                            Cell<?> cell = (Cell<?>) columnData;
                            if (cell.isTombstone())
                                applyColumnDeletion(partitionKey, clusteringColumns, column.name.toString());
                            else
                                applyColumnUpdate(partitionKey, clusteringColumns,
                                                  Optional.of(ColumnValue.from(column, cell.buffer())));
                        }
                    }
                }
                else
                {
                    applyRowDeletion(partitionKey, clusteringColumns);
                }
            }
        }
        else
        {
            applyPartitionDeletion(partitionKey);
        }
    }

    /** Handle an INSERT/UPDATE of a single column (empty {@code columnValue} = a row marker with no cells). */
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        throw new InvalidRequestException(String.format("Column modification is not supported by table %s", metadata()));
    }

    /** Handle deletion of a single column (a cell tombstone). */
    protected void applyColumnDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns, String columnName)
    {
        throw new InvalidRequestException(String.format("Column deletion is not supported by table %s", metadata()));
    }

    /** Handle deletion of a whole row. */
    protected void applyRowDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns)
    {
        throw new InvalidRequestException(String.format("Row deletion is not supported by table %s", metadata()));
    }

    /** Handle deletion of a whole partition. */
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        throw new InvalidRequestException(String.format("Partition deletion is not supported by table %s", metadata()));
    }

    /** The decoded values of a set of columns (a partition key or a clustering). */
    public static final class ColumnValues
    {
        private final List<ColumnMetadata> columns;
        private final ByteBuffer[] values;

        private ColumnValues(List<ColumnMetadata> columns, ByteBuffer[] values)
        {
            this.columns = columns;
            this.values = values;
        }

        private static ColumnValues from(TableMetadata metadata, DecoratedKey partitionKey)
        {
            ByteBuffer[] values = metadata.partitionKeyType instanceof CompositeType
                                  ? ((CompositeType) metadata.partitionKeyType).split(partitionKey.getKey())
                                  : new ByteBuffer[]{ partitionKey.getKey() };
            return new ColumnValues(metadata.partitionKeyColumns(), values);
        }

        private static ColumnValues from(TableMetadata metadata, Clustering<?> clustering)
        {
            return new ColumnValues(metadata.clusteringColumns(), clustering.getBufferArray());
        }

        public int size()
        {
            return columns.size();
        }

        public String name(int index)
        {
            return columns.get(index).name.toString();
        }

        @SuppressWarnings("unchecked")
        public <T> T value(int index)
        {
            return (T) columns.get(index).type.compose(values[index]);
        }
    }

    /** A single decoded column value from an update. */
    public static final class ColumnValue
    {
        private final ColumnMetadata column;
        private final ByteBuffer value;

        private ColumnValue(ColumnMetadata column, ByteBuffer value)
        {
            this.column = column;
            this.value = value;
        }

        private static ColumnValue from(ColumnMetadata column, ByteBuffer value)
        {
            return new ColumnValue(column, value);
        }

        public String name()
        {
            return column.name.toString();
        }

        @SuppressWarnings("unchecked")
        public <T> T value()
        {
            return (T) column.type.compose(value);
        }
    }
}
