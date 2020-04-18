package com.nitesh.parking;

import com.nitesh.parking.exceptions.ValidationException;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws InterruptedException {

        SparkJob sparkJob = new SparkJob(args);
        try {
            sparkJob.startJob();
        } catch (ValidationException | IOException e) {
            e.printStackTrace();
        }
    }
}
