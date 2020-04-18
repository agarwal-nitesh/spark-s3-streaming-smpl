package com.nitesh.parking.config;

public class CustomConstants {

    public static final String URL_DELIMITER = "/";
    public static final String HDP_FS_S3_IMPL_KEY = "fs.s3a.impl";
    public static final String HADOOP_FS_S3_IMPL_KEY = "spark.hadoop.fs.s3a.impl";
    public static final String HDP_FS_S3_IMPL_VALUE = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    public static final String HDP_FS_S3_AWS_ACCESS_KEY_KEY = "fs.s3a.access.key";
    public static final String HDP_FS_S3_AWS_ACCESS_SECRET_KEY = "fs.s3a.secret.key";
    public static final String COLUMN_DOUBLE_UDF_NAME = "recovered";
    public static final String COUNTRY = "country";
    public static final String RECOVERED_COLUMN_NAME = "recovered";
}
