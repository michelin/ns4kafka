package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.LoginSubcommand.BearerAccessRefreshToken;
import com.michelin.ns4kafka.cli.LoginSubcommand.UsernameAndPasswordRequest;

import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;

@Client("http://localhost:8080")
public interface LoginClient {

    @Post("/login")
    BearerAccessRefreshToken login(UsernameAndPasswordRequest request);
}
