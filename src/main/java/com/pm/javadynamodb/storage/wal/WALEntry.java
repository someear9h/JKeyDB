package com.pm.javadynamodb.storage.wal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.pm.javadynamodb.core.model.Item;

// This annotation tells Jackson to not include fields that are null in the JSON output.
// This will keep log file clean.
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WALEntry(
        OperationType operationType,
        String tableName,

        // field for CREATE_TABLE
        String partitionKeyName,
        String sortKeyName,

        // field for PUT/DELETE
        Item item
) {
    // constructor for PUT/DELETE
    public static WALEntry forItem(OperationType op, String table, Item item) {
        return new WALEntry(op, table, null, null, item);
    }

    // Constructor for CREATE_TABLE
    public static WALEntry forTable(String table, String pkName, String skName) {
        return new WALEntry(OperationType.CREATE_TABLE, table, pkName, skName, null);
    }
}
