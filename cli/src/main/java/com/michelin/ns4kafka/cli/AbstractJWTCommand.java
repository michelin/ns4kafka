package com.michelin.ns4kafka.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import io.micronaut.core.io.IOUtils;

public abstract class AbstractJWTCommand {

    public String getJWT() {
        BufferedReader in;
        String jwt = null;
        try {
            in = new BufferedReader(new FileReader("jwt"));
            jwt = IOUtils.readText(in);
        } catch (FileNotFoundException e) {
            System.out.println("Please login first.");
        } catch (IOException e) {
            System.out.println("Please login first.");
        }
        return jwt;
    }

}
