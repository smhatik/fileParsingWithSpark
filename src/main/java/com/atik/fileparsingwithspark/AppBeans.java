package com.atik.fileparsingwithspark;

import com.atik.fileparsingwithspark.service.KafkaSender;
import com.atik.fileparsingwithspark.service.SparkStreaming;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.springframework.web.servlet.view.JstlView;

import java.util.Collections;

@Configuration
@PropertySource("classpath:application.properties")
public class AppBeans {

	@Value("${mongodb.testDB.host}")
	String mongodbHost;

	@Value("${mongodb.testDB.port}")
	String mongodbPort;

	@Value("${mongodb.testDB.database}")
	String mongodbDatabase;

	@Value("${mongodb.testDB.username}")
	String mongodbUsername;

	@Value("${mongodb.testDB.password}")
	String mongodbPassword;


	@Bean
	public KafkaSender initProducer() {
		return new KafkaSender();
	}

	@Bean
	public SparkStreaming initSparkStreaming() {
		return new SparkStreaming();
	}


	@Bean
	public MongoTemplate testDbMongoTemplate() {
		MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(
				this.getMongoClient(this.mongodbHost, this.mongodbPort, this.mongodbUsername, this.mongodbPassword),
				this.mongodbDatabase
		);
		return new MongoTemplate(mongoDbFactory);
	}

	private MongoClient getMongoClient(String host, String port, String username, String password) {
		if (username.isEmpty() && password.isEmpty()) {
			return new MongoClient(host, Integer.parseInt(port));
		} else {
			MongoCredential mongoCredential = MongoCredential.createCredential(this.mongodbUsername, this.mongodbDatabase, this.mongodbPassword.toCharArray());
			ServerAddress serverAddress = new ServerAddress(this.mongodbHost, Integer.parseInt(this.mongodbPort));
			return new MongoClient(serverAddress, Collections.singletonList(mongoCredential));
		}
	}
}
