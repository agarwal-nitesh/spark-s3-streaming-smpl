package com.nitesh.parking.utils;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static com.nitesh.parking.config.CustomConstants.COLUMN_DOUBLE_UDF_NAME;

public class UDFUtil {

    private SQLContext sqlContext;

    public UDFUtil(SQLContext _sqlContext) {
        this.sqlContext = _sqlContext;
    }

    public void registerColumnDoubleUDF() {

        this.sqlContext.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF1<Integer, Integer>)
            (columnValue) -> columnValue * 2, DataTypes.IntegerType);

    }
}
