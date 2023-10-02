package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.services.clients.schema.entities.TagSpecs;
import org.mockito.ArgumentMatcher;

import java.util.List;

public class TagSpecsArgumentMatcher implements ArgumentMatcher<List<TagSpecs>> {

    private List<TagSpecs> left;

    public TagSpecsArgumentMatcher(List<TagSpecs> tagSpecsList) {
        this.left = tagSpecsList;
    }

    @Override
    public boolean matches(List<TagSpecs> right) {
        if(left.size() != right.size()) {
            return false;
        }
        return left.get(0).entityName().equals(right.get(0).entityName()) &&
                left.get(0).entityType().equals(right.get(0).entityType()) &&
                left.get(0).typeName().equals(right.get(0).typeName());
    }
}
