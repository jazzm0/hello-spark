package com.sap.hellospark.service;

import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;

public class Factorial implements UDF1<Integer, Long>, Serializable {

    public Factorial() {
    }

    @Override
    public Long call(Integer n) {
        long result = 1;
        for (var i = 2; i <= n; i++)
            result *= i;
        return result;
    }
}
