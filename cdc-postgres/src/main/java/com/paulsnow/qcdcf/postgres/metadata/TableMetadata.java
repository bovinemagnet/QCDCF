package com.paulsnow.qcdcf.postgres.metadata;

import com.paulsnow.qcdcf.model.TableId;

import java.util.List;

/**
 * Metadata describing a database table's structure.
 *
 * @param tableId        the table identifier
 * @param columns        ordered list of column metadata
 * @param primaryKeyColumns names of the primary key columns
 * @param replicaIdentity the replica identity setting (DEFAULT, FULL, NOTHING, INDEX)
 * @author Paul Snow
 * @since 0.0.0
 */
public record TableMetadata(
        TableId tableId,
        List<ColumnMetadata> columns,
        List<String> primaryKeyColumns,
        String replicaIdentity
) {

    public TableMetadata {
        columns = List.copyOf(columns);
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
    }
}
