package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.security.local.LocalUser;
import com.michelin.ns4kafka.security.local.LocalUserAuthenticationProvider;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
public class LocalUserAuthenticationProviderTest {
    @Mock
    SecurityConfig securityConfig;
    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;
    @InjectMocks
    LocalUserAuthenticationProvider localUserAuthenticationProvider;

    @Test
    void authenticateNoMatchUser() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        Mockito.when(securityConfig.getLocalUsers())
                .thenReturn(List.of());

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null, credentials);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        //then

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(0);
    }
    @Test
    void authenticateMatchUserNoMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        Mockito.when(securityConfig.getLocalUsers())
                .thenReturn(List.of(LocalUser.builder()
                        .username("admin")
                        .password("invalid_sha256_signature")
                        .build()));

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null, credentials);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        //then

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(0);
    }
    @Test
    void authenticateMatchUserMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        Mockito.when(securityConfig.getLocalUsers())
                .thenReturn(List.of(LocalUser.builder()
                        .username("admin")
                        .password("8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918")
                        .groups(List.of("admin"))
                        .build()));
        Mockito.when(resourceBasedSecurityRule.computeRolesFromGroups(ArgumentMatchers.any()))
                .thenReturn(List.of());

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null, credentials);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        //then

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        AuthenticationResponse actual = subscriber.values().get(0);
        Assertions.assertTrue(actual.isAuthenticated());
        Assertions.assertTrue(actual.getAuthentication().isPresent());

        Authentication actualUserDetails = actual.getAuthentication().get();
        Assertions.assertEquals("admin", actualUserDetails.getName());
    }
}
