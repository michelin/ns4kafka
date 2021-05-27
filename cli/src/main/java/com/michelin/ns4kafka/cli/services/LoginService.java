package com.michelin.ns4kafka.cli.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.KafkactlCommand;
import com.michelin.ns4kafka.cli.KafkactlConfig;
import com.michelin.ns4kafka.cli.client.BearerAccessRefreshToken;
import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.UserInfoResponse;
import com.michelin.ns4kafka.cli.client.UsernameAndPasswordRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Date;

@Singleton
public class LoginService {
    @Inject
    KafkactlConfig kafkactlConfiguration;

    @Inject
    ClusterResourceClient clusterResourceClient;

    private final String jwtFilePath = System.getProperty("user.home") + "/.kafkactl/jwt";

    private String accessToken = null;

    public String getAuthorization() {
        return "Bearer " + accessToken;
    }

    public boolean doAuthenticate() {
        return isAuthenticated() || login("gitlab", kafkactlConfiguration.getUserToken());
    }

    public boolean isAuthenticated() {
        try {
            // 0. JWT token file exists
            if(!Files.exists(Path.of(jwtFilePath)))
                return false;
            // 1. Open local JWT token file
            ObjectMapper objectMapper = new ObjectMapper();
            BearerAccessRefreshToken token = objectMapper.readValue(
                    new File(jwtFilePath),
                    BearerAccessRefreshToken.class);
            // 2. Verify token against ns4kafka /user_info endpoint
            UserInfoResponse userInfo = clusterResourceClient.tokenInfo("Bearer " + token.getAccessToken());
            // 3. Display token result

            if (KafkactlCommand.VERBOSE) {
                Date expiry = new Date(userInfo.getExp() * 1000);
                System.out.println("Authentication reused, welcome " + userInfo.getUsername() + "!");
                System.out.println("Your session is valid until " + expiry);
            }
            accessToken = token.getAccessToken();
            return userInfo.isActive();
        } catch (IOException e) {
            // File doesn't exist or File issue or JSON parsing issue
            System.out.println("Unexpected error occurred: " + e.getMessage());
        } catch (HttpClientResponseException e) {
            //401 UNAUTHORIZED OR anything > 400
            if (e.getStatus() != HttpStatus.UNAUTHORIZED) {
                System.out.println("Unexpected error occurred: " + e.getMessage());
            }
        }
        return false;
    }

    public boolean login(String user, String password) {
        try {
            // 1. Call ns4kafka /login
            BearerAccessRefreshToken tokenResponse =
                    clusterResourceClient.login(
                            UsernameAndPasswordRequest
                                    .builder()
                                    .username(user)
                                    .password(password)
                                    .build()
                    );
            // 2. Store token in memory;
            accessToken = tokenResponse.getAccessToken();
            // 3. Display token result
            if (KafkactlCommand.VERBOSE) {
                Calendar calendar = Calendar.getInstance(); // gets a calendar using the default time zone and locale.
                calendar.add(Calendar.SECOND, tokenResponse.getExpiresIn());
                System.out.println("Authentication successful, welcome " + tokenResponse.getUsername() + "!");
                System.out.println("Your session is valid until " + calendar.getTime());
            }
            // 4. Store token result locally
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writeValue(new File(jwtFilePath), tokenResponse);
            }catch(IOException e){
                System.out.println("WARNING : Unexpected error occurred: " + e.getMessage());
            }

            return true;
        } catch (HttpClientResponseException e) {
            System.out.println("Authentication failed with message: " + e.getMessage());
        }
        return false;

    }
}
