package com.pm.javadynamodb.api.dto;

import lombok.Data;

@Data
public class CreateTableRequest {
    private String tableName;
    private String partitionKeyName;
}
