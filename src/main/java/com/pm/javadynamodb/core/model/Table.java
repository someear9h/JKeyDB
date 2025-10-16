package com.pm.javadynamodb.core.model;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class Table {
    private final String tableName;
    private final String partitionKeyName;
    private final Map<String, Item> items;

    /**
     * The actual data store for this table.
     * We use a ConcurrentHashMap because it is thread-safe, which is crucial
     * once we expose this through a multithreaded web server (Spring Boot).
     * Key: The String value of the partition key.
     * Value: The full Item object.
     */

    public Table(String tableName, String partitionKeyName) {
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.items = new ConcurrentHashMap<>();
    }
}
