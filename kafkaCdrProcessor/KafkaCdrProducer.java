package com.trial.kafkacdrproducer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * This program acts as the producer for the topic "cdr"
 * in Kafka. we can change the kafka parameters as well as streaming data  to
 * run it in different configurations 
 */
public class KafkaCdrProducer {

	public static int generateRandomInt(int min, int max) { // Function to
		// generate random integers
		Random rnd = new Random();
		int randomNum = rnd.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	public static float generateRandomFloat() { // Function to
		// generate random floats
		Random rnd = new Random();
		return rnd.nextFloat();
	}

	public static void main(String[] args) {
		long events = Long.parseLong("10");

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.trial.kafkacdrproducer.CustomPartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date today = Calendar.getInstance().getTime();
			String reportDate = df.format(today);
			String[] Account = {"NMDC","SEBI"};
			String accountId = Account[generateRandomInt(0, 1)];
			String[] carrier = {"Airtel","Voda"};
			String carrierId = carrier[generateRandomInt(0, 1)];
			int count = 1;
			String msg = reportDate + "," + accountId + "," + carrierId + ","
					+ count;
			//String msg = reportDate + "," + "SEBI" + "," + "At&T" + "," + count;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"cdr", msg);
			System.out.println(msg);
			producer.send(data);
		}
		producer.close();
	}
}

