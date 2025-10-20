package com.pm.javadynamodb.storage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.javadynamodb.api.exception.TableNotFoundException;
import com.pm.javadynamodb.core.model.Item;
import com.pm.javadynamodb.core.model.Key;
import com.pm.javadynamodb.core.model.Table;
import com.pm.javadynamodb.storage.wal.OperationType;
import com.pm.javadynamodb.storage.wal.WALEntry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

@Service
public class StorageService {

    private static final String WAL_FILE_NAME = "wal.log";
    // a thread safe map to hold all our tables. Key for this map is the table name
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final WALService walService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public StorageService(WALService walService) {
        this.walService = walService;
    }

    /**
     * This method is automatically called by Spring after the service is created.
     * This is where we'll replay the WAL to recover our state.
     */
    @PostConstruct
    public void replayWalOnStartup() {
        System.out.println("Starting WAL replay...");

        try(Stream<String> lines = Files.lines(Paths.get(WAL_FILE_NAME))) {
            lines.forEach(line -> {
                try {
                    WALEntry entry = objectMapper.readValue(line, WALEntry.class);
                    // replay the operation without writing to WAL again
                    applyLogEntry(entry);
                } catch (Exception e) {
                    System.err.println("Failed to replay WAL entry: " + line);
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            // This is expected if the file doesn't exist on first startup.
            System.out.println("WAL file not found, starting with a clean state.");
        }

        System.out.println("WAL replay finished.");
    }

    /**
     * A helper method to apply a log entry to the in-memory state.
     * This logic is used by the startup replay.
     */
    private void applyLogEntry(WALEntry entry) {
        // We need to ensure the table exists, but we don't have its key info here.
        // For simplicity, we'll assume the table exists. A more robust implementation
        // might log table creation too. For now, we manually create tables.
        if(entry.operationType() == OperationType.PUT_ITEM) {
            // We're calling a new private method to avoid re-writing to the WAL.
            performPut(entry.tableName(), entry.item());
        } else if(entry.operationType() == OperationType.DELETE_ITEM) {
            Key key = entry.item().getPrimaryKey();
            performDelete(entry.tableName(), key.getPartitionKey(), key.getSortKey());
        }
    }

    private Table getTable(String tableName) {
        Table table = tables.get(tableName);

        if(table == null) {
            throw new TableNotFoundException("Table " + tableName + " not found");
        }

        return table;
    }

    public void createTable(String tableName, String partitionKeyName, String sortKeyName) {
        if(tables.containsKey(tableName)) {
            System.out.println("Table '" + tableName + "' already exists.");
            return;
        }

        tables.put(tableName, new Table(tableName, partitionKeyName, sortKeyName));
        System.out.println("Table '" + tableName + "' created successfully.");
    }

    public Item putItem(String tableName, Item item) {
        walService.log(new WALEntry(OperationType.PUT_ITEM, tableName, item));
        return performPut(tableName, item);
    }

    public Optional<Item> getItem(String tableName, String partitionKey, String sortKey) {
        Table table = getTable(tableName);
        SortedMap<String, Item> partition = table.getItems().get(partitionKey);
        if (partition == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(partition.get(sortKey));
    }

    public void deleteItem(String tableName, String partitionKey, String sortKey) {
        Optional<Item> itemToDelete = getItem(tableName, partitionKey, sortKey);
        itemToDelete.ifPresent(item -> {
            walService.log(new WALEntry(OperationType.DELETE_ITEM, tableName, item));
            performDelete(tableName, partitionKey, sortKey);
        });
    }

    /**
     * Private helper method that contains the actual logic for putting an item.
     * This avoids writing to the WAL again during a replay.
     */
    private Item performPut(String tableName, Item item) {
        Table table = getTable(tableName);
        String pkName = table.getPartitionKeyName();
        String skName = table.getSortKeyName();

        Object pkValue = item.getAttributes().get(pkName);
        Object skValue = item.getAttributes().get(skName);

        if(pkValue == null || skValue == null) {
            throw new IllegalArgumentException("Item is missing partition or sort key");
        }

        String partitionKey = pkValue.toString();
        String sortKey = skValue.toString();

        item.setPrimaryKey(new Key(partitionKey, sortKey));

        // Get the partition (the inner map). If it doesn't exist, create it.
        SortedMap<String, Item> partition = table.getItems()
                .computeIfAbsent(partitionKey, k -> new ConcurrentSkipListMap<>());

        partition.put(sortKey, item);
        return item;
    }

    /**
     * Private helper method that contains the actual logic for deleting an item.
     */
    private void performDelete(String tableName, String partitionKey, String sortKey) {
        Table table = getTable(tableName);
        SortedMap<String, Item> partition = table.getItems().get(partitionKey);

        if(partition != null) {
            partition.remove(sortKey);

            // Optional: if partition is now empty remove it to save memory
            if(partition.isEmpty()) {
                table.getItems().remove(partitionKey);
            }
        }
    }
}
