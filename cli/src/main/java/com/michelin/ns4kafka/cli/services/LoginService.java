package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.configproperties.KafkactlConfiguration;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class LoginService {
    @Inject
    KafkactlConfiguration kafkactlConfiguration;

    @Inject
    ClusterResourceClient clusterResourceClient;

    public boolean isAuthenticated(){
        return false;
    }
    public AuthenticationResponse login(String user, String password){
        try {
            ClusterResourceClient.BearerAccessRefreshToken tokenResponse =
                    clusterResourceClient.login(
                            ClusterResourceClient.UsernameAndPasswordRequest
                                    .builder()
                                    .username(user)
                                    .password(password)
                                    .build()
                    );
            return AuthenticationResponse.builder()
                    .authenticated(true)
                    .accessToken(tokenResponse)
                    .build();
        } catch(Exception e){
            if(kafkactlConfiguration.isVerbose()){
                System.out.println("verbose");
            }
            return AuthenticationResponse.builder().authenticated(false).build();
        }


    }

    @Builder
    @Getter
    @Setter
    public static class AuthenticationResponse{
        boolean authenticated;
        ClusterResourceClient.BearerAccessRefreshToken accessToken;
    }
}
