package org.example;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Program {
    private static final String EH_CONNECTION_STRING = System.getProperty("EH_CONNECTION_STRING");
    private static final String EVENT_HUB = System.getProperty("EH_NAME");

    /**
     * Main method to demonstrate starting and stopping a {@link EventProcessorClient}.
     *
     * @param args The input arguments to this executable.
     *
     * @throws Exception If there are any errors while running the {@link EventProcessorClient}.
     */
    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(Program.class);
        Consumer<EventContext> processEvent = eventContext -> {
            logger.info("Processing event: Partition Id[{}]; Sequence [{}]",
                    eventContext.getPartitionContext().getPartitionId(),
                    eventContext.getEventData().getSequenceNumber());

            eventContext.updateCheckpoint();
        };

        // This error handler logs the error that occurred and keeps the processor running. If the error occurred in
        // a specific partition and had to be closed, the ownership of the partition will be given up and will allow
        // other processors to claim ownership of the partition.
        Consumer<ErrorContext> processError = errorContext -> {
            logger.error("Error while processing {}, {}, {}, {}", errorContext.getPartitionContext().getEventHubName(),
                    errorContext.getPartitionContext().getConsumerGroup(),
                    errorContext.getPartitionContext().getPartitionId(),
                    errorContext.getThrowable().getMessage());
        };

        Map<String, EventPosition> positions = new HashMap<>();
        IntStream.range(0, 16).forEach(integer -> {
            positions.put(Integer.toString(integer), EventPosition.earliest());
        });

        EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .connectionString(EH_CONNECTION_STRING, EVENT_HUB)
                .initialPartitionEventPosition(positions)
                .processEvent(processEvent)
                .processError(processError)
                .checkpointStore(new SampleCheckpointStore());

        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
        System.out.println("Starting event processor");
        eventProcessorClient.start();

        // Continue to perform other tasks while the processor is running in the background.
        Thread.sleep(TimeUnit.HOURS.toMillis(20));

        System.out.println("Stopping event processor");
        eventProcessorClient.stop();
        System.out.println("Exiting process");
    }
}