package com.practice.kafka.avro.deserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.practice.kafka.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class OrderConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties prop=new Properties();
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("key.deserializer", KafkaAvroDeserializer.class.getSimpleName());
		prop.setProperty("value.deserializer", KafkaAvroDeserializer.class.getSimpleName());
		prop.setProperty("group.id", "OrderGroup");
		prop.setProperty("schema.registry.url", "localhost:8081");
		prop.setProperty("specific.avro.reader", "true");
		
		KafkaConsumer<String,Order> consumer=new KafkaConsumer<>(prop);
		consumer.subscribe(Collections.singletonList("OrderAvroTopic"));
		ConsumerRecords<String,Order> records=consumer.poll(Duration.ofSeconds(30));
		for(ConsumerRecord<String,Order> record:records) {
			Order order=record.value();
			System.out.println(order.getCustomerName());
			System.out.println(order.getProduct());
			System.out.println(order.getQuantity());
			
		}
		consumer.close();
	}

}
