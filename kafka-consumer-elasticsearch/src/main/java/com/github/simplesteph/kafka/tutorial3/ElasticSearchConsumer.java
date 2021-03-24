package com.github.simplesteph.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {

        String hostName = "kafka-course-3426007860.eu-west-1.bonsaisearch.net";
        String username = "";
        String password = "";


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder
                                                     .HttpClientConfigCallback() {
                                                 @Override
                                                 public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                                     return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                                 }
                                             }
                );

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";
//        String topic = "twitter_tweets";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
        //consumer.subscribe(Collections.singleton(topic));
//        consumer.subscribe(Arrays.asList("first","second,..."));

    }


    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        //  String jsonString = "{\"foo\": \"bar\"}";


        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            int recordCount = records.count();
            logger.info("Recieved: " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();


            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();
                //generic
                //String id = record.topic()+"_"+record.partition()+"_"+record.offset();

                try {

                    String id = extractIdFromTweet(jsonString);


                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(jsonString, XContentType.JSON);
                    bulkRequest.add(indexRequest);
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                } catch (NullPointerException ne) {
                    logger.warn("BAd data skipping" + record.value());
                }

//                String id = indexResponse.getId();
//                logger.info(indexResponse.getId());
            }

            if (recordCount > 0) {

                BulkResponse bulkItemResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets!");
                consumer.commitSync();
                logger.info("Offsets committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //close the client gracefully
        //client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String jsonString) {
        return jsonParser.parse(jsonString)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();


    }
}
