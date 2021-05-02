package com.example.kafka.stream.config;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class kafkaStreamConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(kafkaStreamConfig.class);
	
	@Bean
	public void KafkaConfigMethod()
	{
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG,"applicationId");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.STATE_DIR_CONFIG, "SpringBoot-Kafka-Streams-Ex-1");
		
		
		StreamsBuilder streamBuilder = new StreamsBuilder();
		KStream<String,String> kStream = streamBuilder.stream("kafka_Topic"); //Opening the messages to Topic
		

		//Provides bunch of methods to build our own computational logic
		kStream.foreach((k,v) -> System.out.println("Key= " +k + "Value= " + v));
		//kStream.peek((k,v) -> System.out.println("Key= " +k + "Value= " + v));
		
		//Creating a Topology : KafkaStream Computational Logic and provided by Topology Class
		Topology topology = streamBuilder.build();
		
		//Start the Stream
		KafkaStreams streams = new KafkaStreams(topology, config);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() ->
		{
			logger.info("Shutting Down Stream");
			streams.close();
		}	
				) );
	}
	
	

	
	
	
}
