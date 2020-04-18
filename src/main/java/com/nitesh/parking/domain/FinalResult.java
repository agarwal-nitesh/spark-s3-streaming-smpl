package com.nitesh.parking.domain;

import org.apache.spark.unsafe.types.UTF8String;

public class FinalResult {

    private String country;
    private int recovered;

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getRecovered() {
        return recovered;
    }

    public void setRecovered(int recovered) {
        this.recovered = recovered;
    }

    public void setNumber(UTF8String number) {
        this.recovered = Integer.parseInt(number.toString());
    }

    public void setNumberDoubled(UTF8String numberDoubled) {
        this.recovered = Integer.parseInt(numberDoubled.toString());
    }
}
