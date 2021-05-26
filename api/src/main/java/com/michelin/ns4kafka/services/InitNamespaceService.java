package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.InitNamespaceRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Singleton
public class InitNamespaceService {
    @Inject
    InitNamespaceRepository initNamespaceRepository;
    
    public Namespace findNameSpaceByUser(String namespace, String cluster, String user){
        return initNamespaceRepository.getNamespace(namespace, cluster, user);
    }
    
    public RoleBinding findRoleBindingByUser(String namespace, String cluster){
        return initNamespaceRepository.getRoleBindings(namespace, cluster);
    }

    public List<AccessControlEntry> findAclsByUser(String namespace, String user, String cluster, String prefix) throws ExecutionException, InterruptedException, TimeoutException {
        return initNamespaceRepository.getAcls(namespace, user, cluster, prefix);
    }
  
}
