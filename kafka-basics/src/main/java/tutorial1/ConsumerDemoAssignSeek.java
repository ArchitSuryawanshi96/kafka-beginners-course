package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();
        String bootstrapServers="localhost:9092";
        String groupId = "my-seventh-app";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=
                new KafkaConsumer<String, String>(properties);

        TopicPartition partitionToReadFrom =
                new TopicPartition(topic,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        while(keepOnReading){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record:records){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: "+record.key()+
                        "Value: "+record.value()+
                        "Partition: "+record.partition()+
                        "Offset: "+ record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessageToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the app");

    }
}
