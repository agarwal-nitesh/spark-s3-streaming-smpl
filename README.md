# Apache Spark - Stream files from and to AWS S3

This is a simple Java program to illustrate how to stream data to aws s3.

## Build and run


```shell
mvn clean install
```

After this, run the following command to run the project from the same directory:

```shell
java -jar target/parking-1.0-SNAPSHOT.jar <input path> <output path>
```

### Notes:
1. If <input path> and <output path> argument is not given, hardcoded values will be used.
2. Input file format is specified in inputFile.csv
3. Output files will be parquet files. These can be locally viewed using sublime-parquet package.
