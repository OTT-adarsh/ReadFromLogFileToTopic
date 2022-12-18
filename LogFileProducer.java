package kafkaStreams.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class LogFileProducer {

	public static void main(String[] args) {

		Properties properties=new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		KafkaProducer<String,String> producer=new KafkaProducer<>(properties);

		/*
		 * Log File Reader
		 */
		
		File logFile=new File("E:\\\\Adarsh Workspace\\messages.log");
		BufferedReader r=null;
		try {
			r =new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
			String x="";
			String y="";
			int i=0;
			while((y=r.readLine())!=null) {
				i++;
				x=y;
				producer.send(new ProducerRecord<>("myLogTopic",""+i,x));
				System.out.println("Produced :"+x);
			}
			
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		finally {
			try {
				r.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			producer.close();
		}
		
	}

}
