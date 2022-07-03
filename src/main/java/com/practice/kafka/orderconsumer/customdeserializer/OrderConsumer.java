package com.practice.kafka.orderconsumer.customdeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class OrderConsumer {

	public static void main(String[] args) {

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		Properties prop = new Properties();

		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.setProperty("value.deserializer", OrderDeserializer.class.getName());
		prop.setProperty("group.id", "OrderGroup");
		// prop.setProperty("auto.commit.interval.ms", "2000");
		prop.setProperty("auto.commit.offset", "false");
		
		KafkaConsumer<String, Order> consumer = new KafkaConsumer<String, Order>(prop);
		
		class RebalanceHandler implements ConsumerRebalanceListener{

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				
				consumer.commitSync(currentOffsets);
				
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				// TODO Auto-generated method stub
				
			}
			
		}
		
		consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"),new RebalanceHandler());

		try {

			while (true) {
				ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(30));
				int count = 0;
				for (ConsumerRecord<String, Order> record : orders) {
					System.out.println("name:" + record.key());
					System.out.println("Product:" + record.value().getProduct());
					System.out.println("Quantity:" + record.value().getQuantity());
					System.out.println("Partition:" + record.partition());
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1));
					if (count % 10 == 0) {
						consumer.commitAsync(
								currentOffsets,
								new OffsetCommitCallback() {

									@Override
									public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
											Exception exception) {
										if (exception != null)
											System.out.println("commit failed for offset:"+offsets.toString());

									}
								});

					}
				}
			}

		} finally {
			consumer.close();
		}
	}

}
