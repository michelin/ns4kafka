/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.util.exception;

import com.michelin.ns4kafka.model.MetadataResource;
import com.michelin.ns4kafka.util.enumation.Kind;
import java.io.Serial;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Resource validation exception.
 */
@Getter
@AllArgsConstructor
public class ResourceValidationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 32400191899153204L;

    private final Kind kind;

    private final String name;

    private final List<String> validationErrors;

    /**
     * Constructor.
     *
     * @param kind            The kind of the resource
     * @param name            The name of the resource
     * @param validationError The validation error
     */
    public ResourceValidationException(Kind kind, String name, String validationError) {
        this(kind, name, List.of(validationError));
    }

    /**
     * Constructor.
     *
     * @param resource        The resource
     * @param validationError The validation error
     */
    public ResourceValidationException(MetadataResource resource, String validationError) {
        this(resource.getKind(), resource.getMetadata().getName(), List.of(validationError));
    }

    /**
     * Constructor.
     *
     * @param resource         The resource
     * @param validationErrors The validation errors
     */
    public ResourceValidationException(MetadataResource resource, List<String> validationErrors) {
        this(resource.getKind(), resource.getMetadata().getName(), validationErrors);
    }
}
