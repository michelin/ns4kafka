package com.michelin.ns4kafka.services;

import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TopicServiceTest {

    @Mock
    TopicRepository topicRepository;
    @Mock
    AccessControlEntryService acls;

    @InjectMocks
    TopicService topicService;


    Namespace ns = Namespace.builder()
        .metadata(ObjectMeta.builder()
                  .name("test")
                  .cluster("local")
                  .build())
        .build();
    List<AccessControlEntry> aceList = List.of(AccessControlEntry.builder()
                                            .spec(AccessControlEntrySpec.builder()
                                                  .resourceType(ResourceType.TOPIC)
                                                  .resourcePatternType(ResourcePatternType.PREFIXED)
                                                  .permission(Permission.OWNER)
                                                  .grantedTo("test")
                                                  .resource("test_")
                                                  .build())
                                            .build());
    Topic topic = Topic.builder()
        .metadata(ObjectMeta.builder()
                  .name("test_name")
                  .build())
        .build();

    @Test
    void findAllForNamespaceEmptyTopic() {

        when(topicRepository.findAllForCluster(ns.getMetadata().getCluster()))
            .thenReturn(List.of());
        when(acls.findAllGrantedToNamespace(ns)).thenReturn(List.of());

        List<Topic> actual = topicService.findAllForNamespace(ns);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findAllForNamespaceListOfTopic() {

        when(topicRepository.findAllForCluster(ns.getMetadata().getCluster()))
            .thenReturn(List.of(topic));
        when(acls.findAllGrantedToNamespace(ns)).thenReturn(aceList);

        List<Topic> actual = topicService.findAllForNamespace(ns);
        Assertions.assertFalse(actual.isEmpty());
    }

    @Test
    void findByNameExist() {
        when(topicRepository.findAllForCluster(ns.getMetadata().getCluster()))
            .thenReturn(List.of(topic));
        when(acls.findAllGrantedToNamespace(ns)).thenReturn(aceList);

        Optional<Topic> actual = topicService.findByName(ns,"test_name");
        Assertions.assertFalse(actual.isEmpty());
    }

    @Test
    void findByNameNotExist() {
        when(topicRepository.findAllForCluster(ns.getMetadata().getCluster()))
            .thenReturn(List.of());
        when(acls.findAllGrantedToNamespace(ns)).thenReturn(List.of());

        Optional<Topic> actual = topicService.findByName(ns,"test_name");
        Assertions.assertTrue(actual.isEmpty());
    }

}
