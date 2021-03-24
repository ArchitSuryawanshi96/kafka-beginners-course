package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //Create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        for (int i = 0; i<10;i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello world"+Integer.toString(i));

            //send data-async

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error: " + e);

                    }
                }
            });
        }
        //producer.flush();
        producer.close();
    }
}
