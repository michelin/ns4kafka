package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.repository.RoleBindingRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RoleBindingServiceTest {
    @Mock
    RoleBindingRepository roleBindingRepository;

    @InjectMocks
    RoleBindingService roleBindingService;

    @Test
    void findByName() {
        RoleBinding rb1 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb1")
                .cluster("local")
                .build())
            .build();
        RoleBinding rb2 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb2")
                .cluster("local")
                .build())
            .build();
        RoleBinding rb3 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb3")
                .cluster("local")
                .build())
            .build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        var result = roleBindingService.findByName("namespace", "namespace-rb2");
        assertEquals(rb2, result.orElse(null));
    }

    @Test
    void listRoleBindingWithoutParameter() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb3").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace"));
    }

    @Test
    void listRoleBindingWithNameParameter() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb3").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        assertEquals(List.of(rb1), roleBindingService.list("namespace", "namespace-rb1"));
        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace", ""));
        assertEquals(List.of(), roleBindingService.list("namespace", "namespace-rb5"));
    }

    @Test
    void listRoleBindingWithWildcardNameParameter() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb3").build()).build();
        RoleBinding rb4 = RoleBinding.builder().metadata(Metadata.builder().name("rb4").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3, rb4));

        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace", "namespace-*"));
        assertEquals(List.of(rb1, rb2, rb3, rb4), roleBindingService.list("namespace", "*rb?"));
        assertEquals(List.of(rb4), roleBindingService.list("namespace", "rb?"));
        assertEquals(List.of(), roleBindingService.list("namespace", "role_binding*"));
    }
}
