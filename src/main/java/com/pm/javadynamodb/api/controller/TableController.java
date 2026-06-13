package com.pm.javadynamodb.api.controller;

import com.pm.javadynamodb.api.dto.CreateTableRequest;
import com.pm.javadynamodb.storage.service.StorageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/tables")
public class TableController {
    private final StorageService storageService;

    public TableController(StorageService storageService) {
        this.storageService = storageService;
    }

    // endpoint to create a new table
    // POST http://localhost:8080/api/v1/tables
    @PostMapping
    public ResponseEntity<Void> createTable(@RequestBody CreateTableRequest request) {
        storageService.createTable(
                request.getTableName(),
                request.getPartitionKeyName(),
                request.getSortKeyName());

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/{tableName}")
    public ResponseEntity<Void> deleteTable(@PathVariable String tableName) {
        storageService.deleteTable(tableName);
        return ResponseEntity.noContent().build();
    }
}
