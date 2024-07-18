package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.query.RoleBindingFilterParams;
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

        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace"));
    }

    @Test
    void listRoleBindingWithNameParameter() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb3").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        RoleBindingFilterParams params1 = RoleBindingFilterParams.builder().name(List.of("namespace-rb1")).build();
        assertEquals(List.of(rb1), roleBindingService.list("namespace", params1));

        RoleBindingFilterParams params2 = RoleBindingFilterParams.builder().name(List.of("")).build();
        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace", params2));

        RoleBindingFilterParams params3 = RoleBindingFilterParams.builder().name(List.of("namespace-rb5")).build();
        assertEquals(List.of(), roleBindingService.list("namespace", params3));
    }

    @Test
    void listRoleBindingWithWildcardNameParameter() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb3").build()).build();
        RoleBinding rb4 = RoleBinding.builder().metadata(Metadata.builder().name("rb4").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3, rb4));

        RoleBindingFilterParams params1 = RoleBindingFilterParams.builder().name(List.of("namespace-*")).build();
        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace", params1));

        RoleBindingFilterParams params2 = RoleBindingFilterParams.builder().name(List.of("*rb?")).build();
        assertEquals(List.of(rb1, rb2, rb3, rb4), roleBindingService.list("namespace", params2));

        RoleBindingFilterParams params3 = RoleBindingFilterParams.builder().name(List.of("rb?")).build();
        assertEquals(List.of(rb4), roleBindingService.list("namespace", params3));

        RoleBindingFilterParams params4 = RoleBindingFilterParams.builder().name(List.of("role_binding*")).build();
        assertEquals(List.of(), roleBindingService.list("namespace", params4));
    }

    @Test
    void listRoleBindingWithMultipleNameParameters() {
        RoleBinding rb1 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb1").build()).build();
        RoleBinding rb2 = RoleBinding.builder().metadata(Metadata.builder().name("namespace-rb2").build()).build();
        RoleBinding rb3 = RoleBinding.builder().metadata(Metadata.builder().name("ns-rb1").build()).build();
        RoleBinding rb4 = RoleBinding.builder().metadata(Metadata.builder().name("ns-rb2").build()).build();
        RoleBinding rb5 = RoleBinding.builder().metadata(Metadata.builder().name("ns-rb4").build()).build();
        RoleBinding rb6 = RoleBinding.builder().metadata(Metadata.builder().name("rb4").build()).build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3, rb4, rb5, rb6));

        RoleBindingFilterParams params1 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb1", "namespace-rb2")).build();
        assertEquals(List.of(rb1, rb2), roleBindingService.list("namespace", params1));

        RoleBindingFilterParams params2 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb1", "namespace-rb4")).build();
        assertEquals(List.of(rb1), roleBindingService.list("namespace", params2));

        RoleBindingFilterParams params3 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb4", "namespace-rb5")).build();
        assertEquals(List.of(), roleBindingService.list("namespace", params3));

        RoleBindingFilterParams params4 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb1", "namespace-rb1")).build();
        assertEquals(List.of(rb1), roleBindingService.list("namespace", params4));

        RoleBindingFilterParams params5 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb?", "*-rb1")).build();
        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.list("namespace", params5));

        RoleBindingFilterParams params6 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-rb?", "*-rb6")).build();
        assertEquals(List.of(rb1, rb2), roleBindingService.list("namespace", params6));

        RoleBindingFilterParams params7 = RoleBindingFilterParams.builder()
            .name(List.of("namespace-role-binding*", "*-rb6")).build();
        assertEquals(List.of(), roleBindingService.list("namespace", params7));

        RoleBindingFilterParams params8 = RoleBindingFilterParams.builder()
            .name(List.of("*-rb1", "*-rb1")).build();
        assertEquals(List.of(rb1, rb3), roleBindingService.list("namespace", params8));

        RoleBindingFilterParams params9 = RoleBindingFilterParams.builder()
            .name(List.of("ns-rb1", "*-rb2")).build();
        assertEquals(List.of(rb2, rb3, rb4), roleBindingService.list("namespace", params9));

        RoleBindingFilterParams params10 = RoleBindingFilterParams.builder()
            .name(List.of("ns-rb5", "*rb4")).build();
        assertEquals(List.of(rb5, rb6), roleBindingService.list("namespace", params10));

        RoleBindingFilterParams params11 = RoleBindingFilterParams.builder()
            .name(List.of("ns-rb5", "ns-?")).build();
        assertEquals(List.of(), roleBindingService.list("namespace", params11));
    }
}
