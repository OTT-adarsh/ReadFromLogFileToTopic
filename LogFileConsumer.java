package kafkaStreams.sample;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogFileConsumer {

	public static final String bootstrapServer="127.0.0.1:9092";
	public static final String topic="myLogTopic";
	public static final String groupId="myLogId";
	
	public static void main(String[] args) {

		Properties properties=new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
		//properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, value);
		//properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300);
		//properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 300);
		
		/*
		 * Commit Failed Exception
		 */
		
		KafkaConsumer<String, String> kafkaConsumer=new KafkaConsumer<>(properties);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
			//System.out.println("Records poll size "+consumerRecords.records(topic));
			for(ConsumerRecord<String, String> consumerRecord:consumerRecords) {
				System.out.println("Offsets "+consumerRecord.partition()+" : "+consumerRecord.offset());
				System.out.println("Values are "+consumerRecord.value());
			}
		}

	}
}
