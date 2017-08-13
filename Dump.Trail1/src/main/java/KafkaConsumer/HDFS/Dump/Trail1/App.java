package KafkaConsumer.HDFS.Dump.Trail1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class App {
	public static void main(String[] args) throws IOException {
		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox.hortonworks.com:6667");
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);
		TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
		consumer.subscribe(Collections.singletonList("test1"), rebalanceListener);
		HDFSAppendTrial example = new HDFSAppendTrial();
		String coreSite = "/usr/hdp/2.6.0.3-8/hadoop/etc/hadoop/core-site.xml";
		String hdfsSite = "/usr/hdp/2.6.0.3-8/hadoop/etc/hadoop/hdfs-site.xml";
		String hdfsFilePath = "/appendTo/Trial.csv";

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000000);
			for (ConsumerRecord<String, String> record : records) {
				FileSystem fileSystem = example.configureFileSystem(coreSite, hdfsSite);
				String res = example.appendToFile(fileSystem, record.value(), hdfsFilePath);
				System.out.printf("%s\n", record.value());
				if (res.equalsIgnoreCase( "success")) {
		            System.out.println("Successfully appended to file");
		        }
		        else
		            System.out.println("couldn't append to file");
				example.closeFileSystem(fileSystem);

			}
			consumer.commitSync();
	        

		}
	}

	private static class TestConsumerRebalanceListener implements ConsumerRebalanceListener {
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
		}

		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
		}
	}
}
