package com.pm.javadynamodb.storage.wal;

import com.pm.javadynamodb.core.model.Item;

public record WALEntry(
        OperationType operationType,
        String tableName,
        Item item
        // We can add a key here for DELETE if we want, but sending the whole item is simpler for now.
) {}
