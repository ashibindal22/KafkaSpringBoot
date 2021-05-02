package com.example.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.User;

@Service
public class KafkaConsumerListner {
	
	public static Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
	
	@KafkaListener(topics = "kafka_Topic",groupId ="group_id")
	public void consume(Message<String> message)
	{
		
		
		logger.info("Setting up Kafka Listeners");
		System.out.println("Consumed Message: " + message );
		
	}
	
	@KafkaListener(topics = "kafka_Topic_json", groupId = "group_json", containerFactory = "userKafkaListenerFactory")
	public void consumeJson(User user)
	{
		logger.info("Setting Up Kafka Listeners for Json");
		System.out.println("Consumed Json Message: " + user);
	}
}
