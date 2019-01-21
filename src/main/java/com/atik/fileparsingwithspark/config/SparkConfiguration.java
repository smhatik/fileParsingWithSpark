package com.atik.fileparsingwithspark.config;

import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkConfiguration {

	private static SparkSession sparkSession;

	private static JavaSparkContext javaSparkContext;

	private static SQLContext sqlContext;


	static {
		sparkSession = SparkSession.builder()
				.master("local")
				.appName("fileParsingWithSpark")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/testDB.user")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/testDB.user")
				.getOrCreate();

		javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		sqlContext = new SQLContext(javaSparkContext);
	}

	public static SparkSession getSparkSession() {
		return sparkSession;
	}

	public static JavaSparkContext getJavaSparkContext() {
		return javaSparkContext;
	}

	public static SQLContext getSqlContext() {
		return sqlContext;
	}

	public static ReadConfig getReadConfig(String collectionName) {
		Map<String, String> readOverrides = new HashMap<String, String>() {
			{
				put("collection", collectionName);
				put("sampleSize", "1000");
			}
		};
		return ReadConfig.create(javaSparkContext).withOptions(readOverrides);
	}

	public static WriteConfig getWriteConfig(String collectionName) {
		Map<String, String> writeOverrides = new HashMap<String, String>() {
			{
				put("collection", collectionName);
				put("replaceDocument", "true");
			}
		};
		return WriteConfig.create(javaSparkContext).withOptions(writeOverrides);
	}
}
