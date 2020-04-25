package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author supreethmurthy
 * @project kafka-consumer
 */
public class ConsumerDemo {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            logger.error("Invalid input arguments. GroupId and Topic name must be specified while partition is optional.");
            System.exit(-1);
        }
        String groupId = args[0];
        String topicName = args[1];
        Integer partition = -1;
        if (args.length == 3) {
            try {
                partition = Integer.parseInt(args[2]);
            } catch (Exception ex) {
                logger.error("Invalid Partition. Partition should be an integer. Subscribing to all paritions on the topic.");
            }
        }
        // Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Instantiate consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        List<TopicPartition> topicPartitions = new ArrayList<>();

        if (partition != -1) {
            //Subscribe to a specific partition on the topic
            topicPartitions.add(new TopicPartition(topicName, partition));
            consumer.assign(topicPartitions);
        } else {
            //Subscribe to all the partitions on the topic
            consumer.subscribe(Arrays.asList(topicName));
        }
        logger.info("Starting consumer with groupId: {}", groupId);
        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

}
