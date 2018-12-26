package com.orbitz.consul;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public interface PrintFunction {

    default void prettyPrint(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

