package com.pm.javadynamodb.api.dto;

import com.pm.javadynamodb.core.model.Item;

import java.util.List;

public record TableResponse(String tableName, List<Item> items) {
}
