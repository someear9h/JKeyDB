package com.pm.javadynamodb.core.model;

import lombok.Data;
import java.util.Map;

@Data
public class Item {
    private Key primaryKey;
    private Map<String, Object> attributes;
}
