package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationProvider;
import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GitlabAuthenticationProviderTest {
    @Mock
    GitlabAuthenticationService gitlabAuthenticationService;
    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;
    @InjectMocks
    GitlabAuthenticationProvider gitlabAuthenticationProvider;

    @Test
    public void authenticationSuccess(){

        AuthenticationRequest authenticationRequest = new UsernamePasswordCredentials("username","53cu23d_70k3n");
        List<String> groups = List.of("group-1","group-2");
        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret().toString()))
                .thenReturn(Maybe.just("email"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret().toString()))
                .thenReturn(Flowable.fromIterable(groups));
        when(resourceBasedSecurityRule.computeRolesFromGroups(groups))
                .thenReturn(List.of());

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        //then

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        AuthenticationResponse actual = subscriber.values().get(0);
        Assertions.assertTrue(actual.isAuthenticated());
        Assertions.assertTrue(actual.getUserDetails().isPresent());

        UserDetails actualUserDetails = actual.getUserDetails().get();
        Assertions.assertEquals("email", actualUserDetails.getUsername());
        Assertions.assertIterableEquals(groups, (List<String>)actualUserDetails.getAttributes("roles","username").get( "groups"));
        Assertions.assertIterableEquals(List.of(), actualUserDetails.getRoles(),"User has no custom roles");

    }

    @Test
    public void authenticationSuccessAdmin(){

        AuthenticationRequest authenticationRequest = new UsernamePasswordCredentials("admin","53cu23d_70k3n");
        List<String> groups = List.of("group-1","group-2","group-admin");
        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret().toString()))
                .thenReturn(Maybe.just("email"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret().toString()))
                .thenReturn(Flowable.fromIterable(groups));
        when(resourceBasedSecurityRule.computeRolesFromGroups(groups))
                .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        //then

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        AuthenticationResponse actual = subscriber.values().get(0);
        Assertions.assertTrue(actual.isAuthenticated());
        Assertions.assertTrue(actual.getUserDetails().isPresent());

        UserDetails actualUserDetails = actual.getUserDetails().get();
        Assertions.assertEquals("email", actualUserDetails.getUsername());
        Assertions.assertIterableEquals(groups, (List<String>)actualUserDetails.getAttributes("roles","username").get("groups"));
        Assertions.assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN), actualUserDetails.getRoles(),"User has custom roles");

    }

    @Test
    public void authenticationFailure(){

        AuthenticationRequest authenticationRequest = new UsernamePasswordCredentials("admin","f4k3_70k3n");
        List<String> groups = List.of("group-1","group-2","group-admin");
        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret().toString()))
                .thenThrow(new HttpClientResponseException("403 Unauthorized", HttpResponse.unauthorized()));

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        //then
        subscriber.assertError(AuthenticationException.class);
        subscriber.assertValueCount(0);

    }
}
