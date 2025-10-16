package com.pm.javadynamodb.storage.service;

import com.pm.javadynamodb.api.exception.TableNotFoundException;
import com.pm.javadynamodb.core.model.Item;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class StorageServiceTest {

    private StorageService storageService;

    // This method runs before each test, ensuring a clean state
    @BeforeEach
    void setUp() {
        storageService = new StorageService();
        storageService.createTable("Users", "userId");
    }

    @Test
    void putAndGetItem_shouldSucceed() {
        // Arrange: Create a new item
        Item userItem = new Item();
        userItem.setAttributes(Map.of("userId", "user123", "name", "Alice"));

        // Act: Put the item into the storage service
        storageService.putItem("Users", userItem);

        // Assert: Try to get the item back and verify its contents
        Optional<Item> retrievedItemOpt = storageService.getItem("Users", "user123");
        assertTrue(retrievedItemOpt.isPresent(), "Item should be found");

        Item retrievedItem = retrievedItemOpt.get();
        assertEquals("user123", retrievedItem.getAttributes().get("userId"));
        assertEquals("Alice", retrievedItem.getAttributes().get("name"));
        assertNotNull(retrievedItem.getPrimaryKey());
        assertEquals("user123", retrievedItem.getPrimaryKey().getPartitionKey());
    }

    @Test
    void getItem_forNonExistentItem_shouldReturnEmptyOptional() {
        // Act & Assert: Try to get an item that was never added
        Optional<Item> retrievedItemOpt = storageService.getItem("Users", "nonexistent_user");
        assertFalse(retrievedItemOpt.isPresent(), "Item should not be found");
    }

    @Test
    void deleteItem_shouldRemoveItem() {
        // Arrange: Add an item first
        Item userItem = new Item();
        userItem.setAttributes(Map.of("userId", "user_to_delete"));
        storageService.putItem("Users", userItem);

        // Make sure it's there
        assertTrue(storageService.getItem("Users", "user_to_delete").isPresent());

        // Act: Delete the item
        storageService.deleteItem("Users", "user_to_delete");

        // Assert: Ensure the item is now gone
        assertFalse(storageService.getItem("Users", "user_to_delete").isPresent());
    }

    @Test
    void operationOnNonExistentTable_shouldThrowTableNotFoundException() {
        // Act & Assert: All operations should fail for a table that doesn't exist
        assertThrows(TableNotFoundException.class, () -> {
            storageService.getItem("NonExistentTable", "some_key");
        });

        assertThrows(TableNotFoundException.class, () -> {
            storageService.putItem("NonExistentTable", new Item());
        });

        assertThrows(TableNotFoundException.class, () -> {
            storageService.deleteItem("NonExistentTable", "some_key");
        });
    }
}