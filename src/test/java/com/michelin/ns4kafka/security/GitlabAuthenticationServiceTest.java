package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.security.gitlab.GitlabApiClient;
import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationService;
import io.micronaut.http.HttpResponse;
import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class GitlabAuthenticationServiceTest {

    @Mock
    GitlabApiClient gitlabApiClient;

    @InjectMocks
    GitlabAuthenticationService gitlabAuthenticationService;

    @Test
    void findUserSuccess(){
        String token = "v4l1d_70k3n";
        Mockito.when(gitlabApiClient.findUser(token))
                .thenReturn(Flowable.just(Map.of("user","test", "email", "user@mail.com")));

        TestSubscriber<String> subscriber = new TestSubscriber();
        Publisher<String> authenticationResponsePublisher = gitlabAuthenticationService.findUsername(token).toFlowable();

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        String actual = subscriber.values().get(0);
        Assertions.assertEquals("user@mail.com", actual);
    }

    @Test
    void findGroupsOnePage(){
        String token = "v4l1d_70k3n";
        HttpResponse pageOneResponse = HttpResponse
                .ok(List.of(
                        Map.of("full_path", "group1", "unusedKey", "unusedVal"),
                        Map.of("full_path", "group2", "unusedKey", "unusedVal"))
                )
                .header("X-Total-Pages","1");
        Mockito.when(gitlabApiClient.getGroupsPage(token,1)).thenReturn(Flowable.just(pageOneResponse));

        TestSubscriber<String> subscriber = new TestSubscriber();
        Publisher<String> authenticationResponsePublisher = gitlabAuthenticationService.findAllGroups(token);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(2);

        List<String> actual = subscriber.values();
        Assertions.assertIterableEquals(List.of("group1","group2"), actual);
    }
    @Test
    void findGroupsThreePages(){
        String token = "v4l1d_70k3n";
        HttpResponse pageOneResponse = HttpResponse
                .ok(List.of(
                        Map.of("full_path", "group1", "unusedKey", "unusedVal"),
                        Map.of("full_path", "group2", "unusedKey", "unusedVal"))
                )
                .header("X-Next-Page","2")
                .header("X-Total-Pages","3");
        HttpResponse pageTwoResponse = HttpResponse
                .ok(List.of(
                        Map.of("full_path", "group3", "unusedKey", "unusedVal"),
                        Map.of("full_path", "group4", "unusedKey", "unusedVal"))
                )
                .header("X-Next-Page","3")
                .header("X-Total-Pages","3");
        HttpResponse pageThreeResponse = HttpResponse
                .ok(List.of(
                        Map.of("full_path", "group5", "unusedKey", "unusedVal"),
                        Map.of("full_path", "group6", "unusedKey", "unusedVal"))
                )
                .header("X-Total-Pages","3");
        Mockito.when(gitlabApiClient.getGroupsPage(token,1)).thenReturn(Flowable.just(pageOneResponse));
        Mockito.when(gitlabApiClient.getGroupsPage(token,2)).thenReturn(Flowable.just(pageTwoResponse));
        Mockito.when(gitlabApiClient.getGroupsPage(token,3)).thenReturn(Flowable.just(pageThreeResponse));

        TestSubscriber<String> subscriber = new TestSubscriber();
        Publisher<String> authenticationResponsePublisher = gitlabAuthenticationService.findAllGroups(token);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(6);

        List<String> actual = subscriber.values();
        Assertions.assertIterableEquals(List.of("group1","group2","group3","group4","group5","group6"), actual);
    }
}
