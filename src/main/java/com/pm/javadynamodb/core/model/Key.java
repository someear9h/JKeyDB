package com.pm.javadynamodb.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data // Generates getters, setters, toString, etc.
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode // Important for using this as a key in a Map
public class Key {
    private String partitionKey;
    // We will add sortKey here on Day 5
}
