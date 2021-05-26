package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.Topic;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface InitNamespaceRepository {
    
    Namespace getNamespace(String namespace, String cluster, String user);
    
    RoleBinding getRoleBindings(String namespace, String cluster);

    List<AccessControlEntry> getAcls(String namespace, String userName, String cluster, String prefix) throws ExecutionException, InterruptedException, TimeoutException;
}
