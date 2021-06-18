package com.michelin.ns4kafka.services;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.Topic;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micronaut.context.BeanProvider;

@ExtendWith(MockitoExtension.class)
public class NamespaceServiceTest {

    @Mock
    BeanProvider<TopicService> topicServiceStub;
    @Mock
    BeanProvider<RoleBindingService> roleBindingServiceStub;
    @Mock
    BeanProvider<AccessControlEntryService> accessControlEntryServiceStub;
    @Mock
    TopicService topicService;
    @Mock
    RoleBindingService roleBindingService;
    @Mock
    AccessControlEntryService accessControlEntryService;

    @InjectMocks
    NamespaceService namespaceService;

    @BeforeEach
    void init() {
        when(topicServiceStub.get()).thenReturn(topicService);
        when(roleBindingServiceStub.get()).thenReturn(roleBindingService);
        when(accessControlEntryServiceStub.get()).thenReturn(accessControlEntryService);
    }

    @Test
    void isNamespaceEmptySuccess() {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();
        when(topicService.findAllForNamespace(ns)).thenReturn(List.of());
        when(roleBindingService.list("namespace")).thenReturn(List.of());
        when(accessControlEntryService.findAllNamespaceIsGrantor(ns)).thenReturn(List.of());

        var result = namespaceService.isNamespaceEmpty(ns);
        assertTrue(result);

    }

    // @Test
    // void isNamespaceEmptyFailTopicExists() {
    //     TopicService topicService = Mockito.mock(TopicService.class);
    //     RoleBindingService roleBindingService = Mockito.mock(RoleBindingService.class);
    //     AccessControlEntryService accessControlEntryService = Mockito.mock(AccessControlEntryService.class);
    //     // init ns4kfk namespace
    //     Namespace ns = Namespace.builder()
    //             .metadata(ObjectMeta.builder()
    //                     .name("namespace")
    //                     .cluster("local")
    //                     .build())
    //             .spec(NamespaceSpec.builder()
    //                     .connectClusters(List.of("local-name"))
    //                     .build())
    //             .build();

    //     when(topicService.findAllForNamespace(ns)).thenReturn(List.of(new Topic()));

    //     var result = namespaceService.isNamespaceEmpty(ns);
    //     assertFalse(result);

    // }
    // @Test
    // void isNamespaceEmptyFailACLExists() {
    //     TopicService topicService = Mockito.mock(TopicService.class);
    //     RoleBindingService roleBindingService = Mockito.mock(RoleBindingService.class);
    //     AccessControlEntryService accessControlEntryService = Mockito.mock(AccessControlEntryService.class);
    //     // init ns4kfk namespace
    //     Namespace ns = Namespace.builder()
    //             .metadata(ObjectMeta.builder()
    //                     .name("namespace")
    //                     .cluster("local")
    //                     .build())
    //             .spec(NamespaceSpec.builder()
    //                     .connectClusters(List.of("local-name"))
    //                     .build())
    //             .build();

    //     when(topicService.findAllForNamespace(ns)).thenReturn(List.of());
    //     when(accessControlEntryService.findAllNamespaceIsGrantor(ns)).thenReturn(List.of(new AccessControlEntry()));

    //     var result = namespaceService.isNamespaceEmpty(ns);
    //     assertFalse(result);

    // }
    // @Test
    // void isNamespaceEmptyFailRoleBindingExists() {
    //     TopicService topicService = Mockito.mock(TopicService.class);
    //     RoleBindingService roleBindingService = Mockito.mock(RoleBindingService.class);
    //     AccessControlEntryService accessControlEntryService = Mockito.mock(AccessControlEntryService.class);
    //     // init ns4kfk namespace
    //     Namespace ns = Namespace.builder()
    //             .metadata(ObjectMeta.builder()
    //                     .name("namespace")
    //                     .cluster("local")
    //                     .build())
    //             .spec(NamespaceSpec.builder()
    //                     .connectClusters(List.of("local-name"))
    //                     .build())
    //             .build();

    //     when(topicService.findAllForNamespace(ns)).thenReturn(List.of());
    //     when(roleBindingService.list("namespace")).thenReturn(List.of(new RoleBinding()));
    //     when(accessControlEntryService.findAllNamespaceIsGrantor(ns)).thenReturn(List.of());

    //     var result = namespaceService.isNamespaceEmpty(ns);
    //     assertFalse(result);

    // }
}
