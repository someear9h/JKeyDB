package com.pm.javadynamodb.core.model;

import lombok.Data;
import java.util.Map;

@Data
public class Item {
    private Key primaryKey;
    private Map<String, Object> attributes; // like columns in sql & key for this map is column name
}
