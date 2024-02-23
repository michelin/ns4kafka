package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.utils.enums.Kind;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Resource.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resource {
    private String apiVersion;
    private Kind kind;
}
