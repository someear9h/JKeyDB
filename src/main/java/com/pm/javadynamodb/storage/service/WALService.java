package com.pm.javadynamodb.storage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.javadynamodb.storage.wal.WALEntry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Service
public class WALService {
    private static final String WAL_FILE_NAME = "wal.log";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BufferedWriter writer;

    @PostConstruct
    public void init() {
        try {
            // Open the WAL file in append mode. Create it if it doesn't exist.
            this.writer = Files.newBufferedWriter(
                    Paths.get(WAL_FILE_NAME),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            // If we can't write to the log, it's a critical failure.
            throw new RuntimeException("Failed to initialise WAL writer", e);
        }
    }

    public synchronized void log(WALEntry entry) {
        try {
            // Convert the WALEntry object to a JSON string
            String jsonLog = objectMapper.writeValueAsString(entry);
            writer.write(jsonLog);
            writer.newLine(); // Add a newline to separate entries
            writer.flush();   // Ensure the data is written to the disk immediately
        } catch (IOException e) {
            // In a real system, you'd have more robust error handling/retry logic.
            throw new RuntimeException("Failed to write to WAL", e);
        }
    }
}
