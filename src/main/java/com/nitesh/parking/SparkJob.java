package com.nitesh.parking;

import com.amazonaws.auth.AWSCredentials;
import com.nitesh.parking.aws.CredentialsProvider;
import com.nitesh.parking.config.CustomConstants;
import com.nitesh.parking.config.S3Config;
import com.nitesh.parking.domain.FileInputLine;
import com.nitesh.parking.domain.FinalResult;
import com.nitesh.parking.exceptions.ValidationException;
import com.nitesh.parking.utils.FileUtil;
import com.nitesh.parking.utils.S3Util;
import com.nitesh.parking.utils.UDFUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

class SparkJob {

    private final Logger logger = Logger.getLogger(App.class);

    private String[] args;

    private String inputFilePath, outputFolderPath, sparkMaster;

    private UDFUtil udfUtil;
    private S3Config s3Config;
    private S3Util s3Util;
    private FileUtil fileUtil;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private SparkSession sparkSession;
    private StreamingContext streamingContext;

    SparkJob(String[] args) {
        this.args = args;
    }

    void startJob() throws ValidationException, IOException {

        validateArguments();

        Properties properties = new Properties();
        String propFileName = "application.properties";

        InputStream inputStream = App.class.getClassLoader().getResourceAsStream(propFileName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        initialize(properties);

        registerUDFs();

        Dataset<FileInputLine> inputLineDataset = fileUtil.getDatasetFromS3(inputFilePath);

        Dataset<FinalResult> finalResultDataset = getFinalResultDataset(inputLineDataset);

        try {
            fileUtil.writeDatasetToS3File(finalResultDataset, outputFolderPath);
        } catch (StreamingQueryException ex) {
            javaSparkContext.close();
        }
    }

    private Dataset<FinalResult> getFinalResultDataset(Dataset<FileInputLine> inputLineDataset) {

        return inputLineDataset.withColumn(CustomConstants.RECOVERED_COLUMN_NAME,
                callUDF(CustomConstants.COLUMN_DOUBLE_UDF_NAME, col(CustomConstants.RECOVERED_COLUMN_NAME))).as(Encoders.bean(FinalResult.class)
            );
    }

    private void registerUDFs() {
        this.udfUtil.registerColumnDoubleUDF();
    }

    private void initialize(Properties properties) {
        if (args.length >= 2) {
            inputFilePath = args[0];
            outputFolderPath = args[1];
        }
        if (inputFilePath == null) {
            inputFilePath = "public-health-stream";
        }
        if (outputFolderPath == null) {
            outputFolderPath = "public-health";
        }
        sparkMaster = properties.getProperty("spark.master");

        
        S3Config s3Config = getS3Config(properties);
        sparkSession = createJavaSparkContext(s3Config);
        streamingContext = new StreamingContext(sparkSession.sparkContext(), Durations.seconds(100));
        sqlContext = sparkSession.sqlContext();

        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        udfUtil = new UDFUtil(sqlContext);
        s3Util = new S3Util(s3Config, javaSparkContext);
        fileUtil = new FileUtil(s3Util, sparkSession);
    }

    private void validateArguments() throws ValidationException {

        if (this.args.length < 2) {
            logger.error("Invalid arguments.");
            logger.error("1. Input file path.");
            logger.error("2. Output folder path.");
            logger.error("Example: thetechcheck/input/file.csv thetechcheck/output/");

//            throw new ValidationException("Invalid arguments, check help text for instructions.");
        }
    }

    private S3Config getS3Config(Properties properties) {
        s3Config = new S3Config();
        s3Config.setProtocol(properties.getProperty("s3.protocol"));
        s3Config.setBucketName(properties.getProperty("s3.bucketName"));
        s3Config.setAccessKey(properties.getProperty("s3.accessKey"));
        s3Config.setAccessSecret(properties.getProperty("s3.accessSecret"));
        return s3Config;
    }

    private SparkSession createJavaSparkContext(S3Config s3Config) {
        AWSCredentials credentials = CredentialsProvider.getCredentials();
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .config(CustomConstants.HADOOP_FS_S3_IMPL_KEY,
                        CustomConstants.HDP_FS_S3_IMPL_VALUE)
                .config(CustomConstants.HDP_FS_S3_AWS_ACCESS_KEY_KEY,
                        credentials.getAWSAccessKeyId())
                .config(CustomConstants.HDP_FS_S3_AWS_ACCESS_SECRET_KEY,
                        credentials.getAWSSecretKey())
                .getOrCreate();
        return sparkSession;
    }
}
