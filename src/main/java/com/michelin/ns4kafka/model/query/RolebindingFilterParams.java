package com.michelin.ns4kafka.model.query;

import java.util.List;
import lombok.Builder;

 /**
 * Role binding filter parameters in endpoints.
 *
 * @param name role binding name filter
 */
@Builder
public record RolebindingFilterParams(List<String> name) {
}
