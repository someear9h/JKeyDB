package com.pm.javadynamodb.storage.service;

import com.pm.javadynamodb.api.exception.TableNotFoundException;
import com.pm.javadynamodb.core.model.Item;
import com.pm.javadynamodb.core.model.Key;
import com.pm.javadynamodb.core.model.Table;
import com.pm.javadynamodb.storage.wal.OperationType;
import com.pm.javadynamodb.storage.wal.WALEntry;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class StorageService {
    // a thread safe map to hold all our tables. Key for this map is the table name
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final WALService walService;

    public StorageService(WALService walService) {
        this.walService = walService;
    }

    private Table getTable(String tableName) {
        Table table = tables.get(tableName);

        if(table == null) {
            throw new TableNotFoundException("Table " + tableName + " not found");
        }

        return table;
    }

    public void createTable(String tableName, String partitionKeyName) {
        if(tables.containsKey(tableName)) {
            System.out.println("Table '" + tableName + "' already exists.");
            return;
        }

        tables.put(tableName, new Table(tableName, partitionKeyName));
        System.out.println("Table '" + tableName + "' created successfully.");
    }

    public Item putItem(String tableName, Item item) {
        Table table = getTable(tableName);
        String partitionKeyName = table.getPartitionKeyName();
        Object partitionKeyValue = item.getAttributes().get(partitionKeyName);

        if (partitionKeyValue == null) {
            throw new IllegalArgumentException("Item is missing partition key: " + partitionKeyName);
        }

        // set the primary key on the item object
        String pkValue = partitionKeyValue.toString();
        item.setPrimaryKey(new Key(pkValue));

        // 1. Log the operation BEFORE changing memory. This is the "Write-Ahead" part.
        walService.log(new WALEntry(OperationType.PUT_ITEM, tableName, item));

        // Now, safely update the in-memory map
        table.getItems().put(pkValue, item);
        return item;
    }

    public Optional<Item> getItem(String tableName, String partitionKey) {
        Table table = getTable(tableName);
        // Optional.ofNullable handles the case where the item might not exist
        return Optional.ofNullable(table.getItems().get(partitionKey));
    }

    public void deleteItem(String tableName, String partitionKey) {
        Table table = getTable(tableName);

        // We need the full item to log it, so we retrieve it first.
        Item itemToDelete = table.getItems().get(partitionKey);

        if(itemToDelete != null) {
            // WAL logic
            // 1. log the delete operation before changing memory
            walService.log(new WALEntry(OperationType.DELETE_ITEM, tableName, itemToDelete));

            // 2. safely update the in-memory map
            table.getItems().remove(partitionKey);
        }
    }
}
