package com.pm.javadynamodb.api.controller;

import com.pm.javadynamodb.api.dto.CreateTableRequest;
import com.pm.javadynamodb.core.model.Item;
import com.pm.javadynamodb.storage.service.StorageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

// @RestController marks this class as a request handler.
// @RequestMapping("/api/v1") sets a base path for all endpoints in this controller.
@RestController
@RequestMapping("/api/v1")
public class DatabaseController {

    private final StorageService storageService;

    public DatabaseController(StorageService storageService) {
        this.storageService = storageService;
    }

    // endpoint to create a new table
    // POST http://localhost:8080/api/v1/tables
    @PostMapping("/tables")
    public ResponseEntity<Void> createTable(@RequestBody CreateTableRequest request) {
        storageService.createTable(
                request.getTableName(),
                request.getPartitionKeyName(),
                request.getSortKeyName());

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    // Endpoint to add or update an item in a table.
    // POST http://localhost:8080/api/v1/tables/Users/items
    @PostMapping("/tables/{tableName}/items")
    public ResponseEntity<Item> putItem(
            @PathVariable String tableName,
            @RequestBody Map<String, Object> attributes) {
        Item item = new Item();
        item.setAttributes(attributes);
        Item savedItem = storageService.putItem(tableName, item);
        return ResponseEntity.ok(savedItem);
    }

    // Endpoint to retrieve an item from a table.
    // GET http://localhost:8080/api/v1/tables/Users/items/user123
    @GetMapping("/tables/{tableName}/items/{partitionKey}/{sortKey}")
    public ResponseEntity<Item> getItem(
            @PathVariable String tableName, // @PathVariable gets the tableName directly from the url
            @PathVariable String partitionKey,
            @PathVariable String sortKey) {

        Optional<Item> item = storageService.getItem(tableName, partitionKey, sortKey);
        // A clean way to return the item if present, or a 404 Not Found otherwise.
        return item.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // Endpoint to delete an item from a table.
    // DELETE http://localhost:8080/api/v1/tables/Users/items/user123
    @DeleteMapping("/tables/{tableName}/items/{partitionKey}/{sortKey}")
    public ResponseEntity<Void> deleteItem(
            @PathVariable String tableName,
            @PathVariable String partitionKey,
            @PathVariable String sortKey) {

        storageService.deleteItem(tableName, partitionKey, sortKey);
        // A 204 No Content response is standard for a successful DELETE.
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/tables/{tableName}/items")
    public ResponseEntity<Collection<Item>> queryItems(
            @PathVariable String tableName, @RequestParam String partitionKey,
            @RequestParam(required = false) String startKey,
            @RequestParam(required = false) String endKey
    ) {
        Collection<Item> items = storageService.query(tableName, partitionKey, startKey, endKey);

        return ResponseEntity.ok(items);
    }
}
