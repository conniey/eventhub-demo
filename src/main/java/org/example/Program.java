package org.example;

import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Program {
    private static final AtomicInteger PARTITION_0 = new AtomicInteger();
    private static final AtomicInteger PARTITION_1 = new AtomicInteger();
    private static final Map<String, AtomicInteger> ATOMIC_INTEGER_MAP = new HashMap<>();

    /**
     * Main method to demonstrate starting and stopping a {@link EventProcessorClient}.
     *
     * @param args The input arguments to this executable.
     *
     * @throws Exception If there are any errors while running the {@link EventProcessorClient}.
     */
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Program.class);

        ATOMIC_INTEGER_MAP.put("0", PARTITION_0);
        ATOMIC_INTEGER_MAP.put("1", PARTITION_1);

        final Consumer<EventContext> processEvent = eventContext -> {
            System.out.printf("Processing event. partitionId[%s] sequence[%s]%n",
                    eventContext.getPartitionContext().getPartitionId(),
                    eventContext.getEventData().getSequenceNumber());

            eventContext.updateCheckpoint();
        };

        final Consumer<EventBatchContext> processEvents = b -> {
            // System.out.println("Batch received. Size: " + b.getEvents());
            // System.out.println("Batch properties:" + b.getLastEnqueuedEventProperties());

            final String partitionId = b.getPartitionContext().getPartitionId();
            final AtomicInteger atomicInteger = ATOMIC_INTEGER_MAP.get(partitionId);
            if (atomicInteger == null) {
                throw new IllegalArgumentException("Partition does not exist. " + partitionId);
            }

            atomicInteger.addAndGet(b.getEvents().size());
        };

        final BlobContainerAsyncClient client = new BlobContainerClientBuilder()
                .connectionString(Environment.getStorageConnectionString())
                .containerName(Environment.getStorageContainerName())
                .buildAsyncClient();

        client.exists().flatMap(doesExist -> doesExist ? Mono.empty() : client.create())
                .block(Duration.ofSeconds(30));

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
                .consumerGroup(Environment.getConsumerGroup())
                .connectionString(Environment.getEventHubsConnectionString(), Environment.getEventHubName())
                .initialPartitionEventPosition(positions)
                .trackLastEnqueuedEventProperties(true)
                .processError(processError)
                .checkpointStore(new BlobCheckpointStore(client));

        System.out.printf("Batch Size: %d. Window: %s. Use window? %b%n", Environment.getBatchSize(),
                Environment.getWindowTimeout(), Environment.useWindowTimeout());

        if (Environment.useWindowTimeout()) {
            eventProcessorClientBuilder.processEventBatch(processEvents, Environment.getBatchSize(),
                    Environment.getWindowTimeout());
        } else {
            eventProcessorClientBuilder.processEventBatch(processEvents, Environment.getBatchSize());
        }

        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
        System.out.println("Starting event processor");

        final long startTime = System.currentTimeMillis();

        eventProcessorClient.start();

        // Continue to perform other tasks while the processor is running in the background.
        Thread.sleep(TimeUnit.MINUTES.toMillis(5));

        System.out.println("Stopping event processor");
        eventProcessorClient.stop();

        final long endTime = System.currentTimeMillis();
        final long durationInMs = endTime - startTime;
        final double durationInS = (durationInMs) * 0.001;
        final int p0 = PARTITION_0.get();
        final int p1 = PARTITION_1.get();

        System.out.printf("Start: [%d] End: [%d]. P0 [%d]. P1 [%d].%n", startTime, endTime, p0, p1);
        System.out.printf("P0: [%f] events/s%n", (p0 / durationInS));
        System.out.printf("P1: [%f] events/s%n", (p1 / durationInS));
        System.out.printf("Overall: [%f] events/s%n", (p0 + p1) / durationInS);

        System.out.println("Exiting process");
    }
}