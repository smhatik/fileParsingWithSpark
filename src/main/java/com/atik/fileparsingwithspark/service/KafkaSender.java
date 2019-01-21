/*
* After filling the Properties class with the necessary configuration properties,
* we can use it to create an object of KafkaProducer. Whenever we want to send a message to the Kafka server after that,
* we'll create an object of ProducerRecord and call the KafkaProducer's send() method with that record to send the message.
* The ProducerRecord takes two parameters: the name of the topic to which message should be published, and the actual message.
* Don't forget to call the Producer.close() method when you're done using the producer
* */

package com.atik.fileparsingwithspark.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Configuration
@PropertySource("classpath:application.properties")
public class KafkaSender {

	@Value("${brokerList}")
	private String brokerList;

	@Value("${topic.path}")
	private String topicPath;

	private Producer<String, String> producerForPath;

	public KafkaSender() {
	}

	@PostConstruct
	public void initIt() {
		Properties kafkaPropsForPath = new Properties() {
			{
				put("bootstrap.servers", brokerList);
				put("key.serializer",
						"org.apache.kafka.common.serialization.StringSerializer");
				put("value.serializer",
						"org.apache.kafka.common.serialization.StringSerializer");
				put("acks", "1");
				put("retries", "1");
				put("linger.ms", 5);
			}
		};
		producerForPath = new KafkaProducer<>(kafkaPropsForPath);

	}

	public void send(String path) throws ExecutionException,
			InterruptedException {
		sendSync(path);
	}

	private void sendSync(String value) throws ExecutionException,
			InterruptedException {
		ProducerRecord<String, String> record = new ProducerRecord<>(topicPath, value);
		producerForPath.send(record).get();
		producerForPath.close();
	}
}