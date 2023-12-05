package com.example.kafkaproducer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class MediaChangesProducer {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaChangesProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public MediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() {

        // to read ral time stream data from media, we use event source
        BackgroundEventHandler eventHandler = new MediaChangesHandler(kafkaTemplate, topicName);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";



        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(ConnectStrategy.http(URI.create(url))))
                .build();

        eventSource.start();

        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error %s", e.getMessage()));
            throw new RuntimeException("An error has occurred");
        }

    }

}
