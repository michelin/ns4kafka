package com.michelin.ns4kafka.authentication;

import io.micronaut.security.authentication.Authentication;

import javax.inject.Singleton;
import java.util.List;

@Singleton
public class GitlabAuthorizationService {
    private String apiUrl="https://gitlab.com/api/v4/groups";
    public List<String> findGroups(String token){
        return List.of("XXX", "YYY", "ZZZ");
    }
    public GitlabAuthorizationService(){

    }
}
