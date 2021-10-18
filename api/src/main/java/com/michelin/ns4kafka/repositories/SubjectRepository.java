package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Subject;

import java.util.List;

public interface SubjectRepository {
    Subject create(Subject subject);
    List<Subject> findAllForCluster(String cluster);
    void delete(Subject subject);
}
