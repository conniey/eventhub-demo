package org.example;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Sender {
    public static void main(String[] args) {
        System.out.println("Running program.");

        // create a producer client
        try (EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(Environment.getEventHubsConnectionString(), Environment.getEventHubName())
                .buildProducerClient()) {

            while (true) {
                System.out.println("Sending events to " + producer.getEventHubName());
                publishEvents(producer, 1500);

                if (true) {
                    try {
                        System.out.println("Sleeping...");
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        System.out.println("Couldn't sleep: " + e);
                        break;
                    }
                } else {
                    break;
                }
            }
        } finally {
            System.out.println("Closing producer.");
        }

        System.out.println("Quitting.");
    }

    /**
     * Code sample for publishing events.
     *
     * @throws IllegalArgumentException if the EventData is bigger than the max batch size.
     */
    public static void publishEvents(EventHubProducerClient producer, int numberOfEvents) {
        EventDataBatch eventDataBatch = producer.createBatch();

        byte[] contents = new byte[500];
        Arrays.fill(contents, (byte) 'a');

        System.out.println("Publishing: " + numberOfEvents);
        final long start = System.nanoTime();

        int printCounter = 0;
        for (int i = 0; i < numberOfEvents; i++) {
            EventData eventData = new EventData(contents);

            // try to add the event from the array to the batch
            if (!eventDataBatch.tryAdd(eventData)) {
                // if the batch is full, send it and then create a new batch
                producer.send(eventDataBatch);
                printCounter++;
                if ((printCounter % 100) == 0) {
                    System.out.println("- " + i);
                }

                eventDataBatch = producer.createBatch();

                // Try to add that event that couldn't fit before.
                if (!eventDataBatch.tryAdd(eventData)) {
                    throw new IllegalArgumentException("Event is too large for an empty batch. Max size: "
                            + eventDataBatch.getMaxSizeInBytes());
                }
            }
        }

        // send the last batch of remaining events
        if (eventDataBatch.getCount() > 0) {
            producer.send(eventDataBatch);
            System.out.println("Sending last done.");
        }

        final long end = System.nanoTime();
        final long durationNs = end - start;
        final Duration duration = Duration.ofNanos(durationNs);
        System.out.printf("Complete. Duration: %s%n", duration);
    }
}