package com.example.KafkaTwitterProducer;


import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.*;
import twitter4j.conf.*;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A Kafka Producer that gets tweets on certain keywords
 * from twitter datasource and publishes to a kafka topic
 *
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <topic-name> <keyword_1> ... <keyword_n>
 * <comsumerKey>	- Twitter consumer key
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>	- Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * <topic-name>		- The kafka topic to subscribe to
 * <keyword_1>		- The keyword to filter tweets
 * <keyword_n>		- Any number of keywords to filter tweets
 *
 * More discussion at stdatalabs.blogspot.com
 *
 * @author Sachin Thirumala
 * Outputs (Tweet ID + " " + Tweet Created AT + " " + Tweet Content + " " + Tweet Hashtag) to consumer
 *
 */

public class KafkaTwitterProducer {
    public static void main(String[] args) throws Exception {
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

        String consumerKey = "UvDagOJuMnxEqsuhTNaTFq9V6";
        String consumerSecret = "XbSQopO4kdJHZrdETrupDZMzLLZ7FwbNhWYMOc6vcK3rjrgKYt";
        String accessToken = "2255042673-RYwCsd6eoksoNq0b8hi1h7R7g5VIlg4zIDB9Xj3";
        String accessTokenSecret = "FUwdobwnh3Eo51KVxrLktZhiYXVSqQ1Lz4iitjRkOxmO5";
        String topicName = "Twitter-API";
        String[] arguments = args.clone();
        String[] keyWords = {"#AI", "#MachineLearning", "#ArtificialIntelligence"};

        // Set twitter oAuth tokens in the configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

        // Create twitterstream using the configuration
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);

        // Filter keywords
        FilterQuery query = new FilterQuery().track(keyWords);
        twitterStream.filter(query);

        // Thread.sleep(5000);

        // Add Kafka producer config settings
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
        int j = 0;

        // poll for new tweets in the queue. If new tweets are added, send them
        // to the topic
        while (true) {
            Status ret = queue.poll();

            if (ret == null || !ret.getLang().equals("en")) {
                Thread.sleep(10);
                // i++;
            } else {
                System.out.println();
                System.out.println("Tweet:" + ret);
                System.out.println("Tweet ID:" + ret.getId());
                System.out.println("Created At:" + ret.getCreatedAt());
                System.out.println("Tweet Content:" + ret.getText());
                System.out.println();
                producer.send(new ProducerRecord<String,String>(topicName, Integer.toString(j++),ret.toString()));
            }
        }
    }
    // producer.close();
    // Thread.sleep(500);
    // twitterStream.shutdown();
}