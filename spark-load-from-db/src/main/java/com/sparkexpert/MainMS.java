package com.sparkexpert;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.System;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/*

  Make sure to set the following env variables before running the app:

  export SQLSERVER_USERNAME = "<your sqlserver user name>"
  export SQLSERVER_PWD = "<your sqlserver password>"
  export SQLSERVER_HOST_PORT = "<your sqlserver host>:1433"
  export SQLSERVER_DB = "<your sqlserver database>"
  export SQLSERVER_TABLE = "<your sqlserver table>"
  export AWS_ACCESS_KEY_ID = "<your AWS key>"
  export AWS_SECRET_ACCESS_KEY = "<your AWS secret>"
  export AWS_S3_BUCKET = "<your S3 bucket for saving data>"

*/

public class MainMS implements Serializable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String SQLSERVER_USERNAME = System.getenv("SQL_USERNAME");
    private static final String SQLSERVER_PWD = System.getenv("SQL_PWD");
    private static final String SQLSERVER_HOST_PORT = System.getenv("SQL_HOST_PORT");
    private static final String SQLSERVER_DB = System.getenv("SQL_DB");
    private static final String SQLSERVER_TABLE = System.getenv("SQL_TABLE");
    private static final String SQLSERVER_CONNECTION_URL =
            "jdbc:sqlserver://" + SQLSERVER_HOST_PORT + ";database=" + SQLSERVER_DB + ";user=" + SQLSERVER_USERNAME + ";password=" + SQL_PWD + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
    
    private static final String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
    private static final String AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
    private static final String AWS_S3_BUCKET = System.getenv("S3_BUCKET");

    private static final JavaSparkContext sc =
            new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);

    public static void main(String[] args) {
        // Data source options
        Map<String, String> options = new HashMap<>();
        options.put("driver", SQLSERVER_DRIVER);
        options.put("url", SQLSERVER_CONNECTION_URL);
        options.put("dbtable", SQLSERVER_TABLE);

        // Load dataframe with jdbc options  
        DataFrame jdbcDF = sqlContext.load("jdbc", options);

        List<Row> customerRows = jdbcDF.collectAsList();

        for (Row customerRow : customerRows) {
            LOGGER.info(customerRow);
        }
        LOGGER.info(customerRows.size());

        // Save dataframe query to S3
        jdbcDF.select("*").write().format("com.databricks.spark.csv").
          save("s3n://" + AWS_ACCESS_KEY_ID + ":" + AWS_ACCESS_KEY_ID + "@" + AWS_S3_BUCKET);
        
        LOGGER.info("Data saved to s3 bucket: "+AWS_S3_BUCKET);
    }
}
