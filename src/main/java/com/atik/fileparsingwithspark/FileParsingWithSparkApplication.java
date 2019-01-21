package com.atik.fileparsingwithspark;

import com.atik.fileparsingwithspark.service.SparkStreaming;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;

@SpringBootApplication
public class FileParsingWithSparkApplication implements CommandLineRunner {

	@Autowired
	private SparkStreaming sparkStreaming;

	@Autowired
	@Qualifier("testDbMongoTemplate")
	private MongoTemplate testDbMongoTemplate;

	public static void main(String[] args) {
		SpringApplication.run(FileParsingWithSparkApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		sparkStreaming.startSparkStreaming(testDbMongoTemplate);
	}

}

