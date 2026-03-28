package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.TableId;

import java.util.List;

/**
 * Cached metadata for a PostgreSQL relation (table) received via a pgoutput 'R' message.
 * <p>
 * Each time a Relation message is received on the replication stream, an instance of this
 * record is stored in the decoder's cache so that subsequent Insert, Update, and Delete
 * messages can resolve column names and key information.
 *
 * @param relationId      the PostgreSQL OID of the relation
 * @param tableId         the schema-qualified table identity
 * @param replicaIdentity the replica identity setting ('d' default, 'f' full, 'i' index, 'n' nothing)
 * @param columns         ordered list of column metadata
 * @author Paul Snow
 * @since 0.0.0
 */
public record RelationMetadata(
        int relationId,
        TableId tableId,
        char replicaIdentity,
        List<ColumnInfo> columns
) {

    /**
     * Metadata for a single column within a relation.
     *
     * @param name      the column name
     * @param partOfKey whether this column is part of the replica identity key
     * @param typeOid   the PostgreSQL type OID
     */
    public record ColumnInfo(String name, boolean partOfKey, int typeOid) {}

    /**
     * Returns the names of columns that form part of the replica identity key.
     *
     * @return an unmodifiable list of key column names
     */
    public List<String> keyColumnNames() {
        return columns.stream()
                .filter(ColumnInfo::partOfKey)
                .map(ColumnInfo::name)
                .toList();
    }
}
