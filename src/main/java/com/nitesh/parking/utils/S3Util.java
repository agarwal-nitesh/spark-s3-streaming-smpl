package com.nitesh.parking.utils;

import com.nitesh.parking.config.S3Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.nitesh.parking.config.CustomConstants.*;

public class S3Util {
    private Logger logger = LoggerFactory.getLogger(S3Util.class);
    private S3Config s3Config;
    private JavaSparkContext javaSparkContext;

    public S3Util(S3Config _s3Config, JavaSparkContext _javaSparkContext) {
        this.s3Config = _s3Config;
        this.javaSparkContext = _javaSparkContext;
    }

    String getS3FileURLFromPath(String filePath) {
        String fileUrl = this.s3Config.getProtocol() + filePath;// + URL_DELIMITER + filePath;
        logger.info("S3 File url " + fileUrl);
        return fileUrl;
    }

    void setHadoopS3Configuration() {
        logger.info("setHadoopS3Configuration :: ");
        Configuration hadoopConfig = this.javaSparkContext.hadoopConfiguration();
        hadoopConfig.set(HDP_FS_S3_IMPL_KEY, HDP_FS_S3_IMPL_VALUE);
        hadoopConfig.set(HDP_FS_S3_AWS_ACCESS_KEY_KEY, s3Config.getAccessKey());
        hadoopConfig.set(HDP_FS_S3_AWS_ACCESS_SECRET_KEY, s3Config.getAccessSecret());
    }
}
