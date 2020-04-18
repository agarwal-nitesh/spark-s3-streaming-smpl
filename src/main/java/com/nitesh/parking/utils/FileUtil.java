package com.nitesh.parking.utils;

import com.nitesh.parking.domain.FileInputLine;
import com.nitesh.parking.domain.FinalResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static com.nitesh.parking.config.CustomConstants.URL_DELIMITER;

public class FileUtil {

    private S3Util s3Util;
    private SparkSession sparkSession;

    public FileUtil(
            S3Util _s3Config,
            SparkSession _sparkSession) {

        this.s3Util = _s3Config;
        this.sparkSession = _sparkSession;

    }

    public Dataset<FileInputLine> getDatasetFromS3(String filePath) {

        Dataset<FileInputLine> fileDataSet = null;

        String s3FilePath = this.s3Util.getS3FileURLFromPath(filePath);

        this.s3Util.setHadoopS3Configuration();

        /**
         * private int serialNumber;
         *     private String state;
         *     private String country;
         *     private float lat;
         *     private float lon;
         *     private String date;
         *     private int recovered;
         *     private int death;
         */

        StructType schema = new StructType()
                .add("serialNumber", "integer")
                .add("state", "string")
                .add("country", "string")
                .add("lat", "float")
                .add("lon", "float")
                .add("date", "string")
                .add("confirmed", "integer")
                .add("recovered", "integer")
                .add("death", "integer");

        fileDataSet = this.sparkSession.readStream()
                .option("header", "true")
                .schema(schema)
                .csv(s3FilePath + "/*")
                .as(Encoders.bean(FileInputLine.class));

        return fileDataSet;
    }

    public void writeDatasetToS3File(Dataset<FinalResult> dataset, String filePath) throws StreamingQueryException {

        String s3FilePath = this.s3Util.getS3FileURLFromPath(filePath) + URL_DELIMITER;

        StreamingQuery query = dataset
            .writeStream()
            .option("header", "true")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation","checkpoints")
            .option("path", s3FilePath)
            .start();
        query.awaitTermination();
    }
}
