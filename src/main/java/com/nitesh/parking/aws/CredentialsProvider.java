package com.nitesh.parking.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import java.util.UUID;


public class CredentialsProvider {

    private static volatile AWSCredentials credentials = null;

    private static final String profileName = "default";

    public static AWSCredentials getCredentials() {
        if (credentials == null) {
            synchronized (CredentialsProvider.class) {
                if (credentials == null) {
                    credentials = fetchCredentials();
                }
            }
        }
        return credentials;
    }
    private static AWSCredentials fetchCredentials() {
        ProfileCredentialsProvider profileCredentialsProvider = new ProfileCredentialsProvider(profileName);
        return profileCredentialsProvider.getCredentials();
    }
}
