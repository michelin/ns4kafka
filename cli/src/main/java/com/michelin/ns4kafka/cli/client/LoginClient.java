package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.LoginSubcommand.BearerAccessRefreshToken;
import com.michelin.ns4kafka.cli.LoginSubcommand.UsernameAndPasswordRequest;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;

@Client("${ns4kafka.api.url}")
public interface LoginClient {

    @Post("/login")
    BearerAccessRefreshToken login(@Body UsernameAndPasswordRequest request);
}
