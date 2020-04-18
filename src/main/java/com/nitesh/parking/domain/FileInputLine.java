package com.nitesh.parking.domain;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

public class FileInputLine implements Serializable {

    private int serialNumber;
    private String state;
    private String country;
    private float lat;
    private float lon;
    private String date;
    private int confirmed;
    private int recovered;
    private int death;

    public int getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(UTF8String serialNumber) {
        this.serialNumber = Integer.parseInt(serialNumber.toString());
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(UTF8String latString) {
        this.lat = Float.parseFloat(latString.toString());
    }

    public float getLon() {
        return lon;
    }

    public void setLon(UTF8String lonString) {
        this.lon = Float.parseFloat(lonString.toString());
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getRecovered() {
        return recovered;
    }

    public void setRecovered(UTF8String recoveredString) {
        this.recovered = Integer.parseInt(recoveredString.toString());
    }

    public int getDeath() {
        return death;
    }

    public void setDeath(UTF8String deathString) {
        this.death = Integer.parseInt(deathString.toString());
    }

    public int getConfirmed() {
        return confirmed;
    }

    public void setConfirmed(int confirmed) {
        this.confirmed = confirmed;
    }
}
