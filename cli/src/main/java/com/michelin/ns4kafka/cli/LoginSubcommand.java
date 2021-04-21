package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.cli.client.LoginClient;

import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.Collection;
import java.util.concurrent.Callable;

@Command(name = "login", description = "Store JSON Web Token return by credentials")
public class LoginSubcommand implements Callable<Integer> {

    @Inject
    LoginClient client;

    @Option(names = {"-u", "--username"}, required = true, description = "Username")
    String username = "Gitlab";

    //TODO change to char[]
    @Option(names = {"-p", "--password"}, required = true, description = "Password")
    String password;

    @Value("${HOME}/.kafkactl/jwt")
    private String path;


    @Override
    public Integer call() throws Exception {
        UsernameAndPasswordRequest request = UsernameAndPasswordRequest.builder()
                .username(username)
                .password(password)
                .build();
        try{
            System.out.println(path);
            BearerAccessRefreshToken response = client.login(request);

            Calendar calendar = Calendar.getInstance(); // gets a calendar using the default time zone and locale.
            calendar.add(Calendar.SECOND, response.getExpiresIn());

            System.out.println("Authentication successful, welcome "+response.getUsername()+ "!");
            System.out.println("Your session is valid until "+calendar.getTime());

            File file = new File(path);
            file.createNewFile();
            FileWriter myWriter = new FileWriter(path);
            myWriter.write(response.getAccessToken());
            myWriter.close();
        } catch(HttpClientResponseException e) {
            System.out.println("Authentication failed with message : "+e.getMessage());
        }
        return 0;
    }

    @Introspected
    @Getter
    @Setter
    @Builder
    public static class UsernameAndPasswordRequest {
        private String username;
        private String password;
    }

    @Introspected
    @Getter
    @Setter
    public static class BearerAccessRefreshToken {
        private String username;
        private Collection<String> roles;

        @JsonProperty("access_token")
        private String accessToken;

        @JsonProperty("token_type")
        private String tokenType;

        @JsonProperty("expires_in")
        private Integer expiresIn;
    }
}
