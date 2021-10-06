package org.example;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import reactor.core.Disposable;

import java.util.function.Consumer;

public class LowLevelReceiver {
    public static void main(String[] args) throws Exception {

        EventHubConsumerAsyncClient client = new EventHubClientBuilder()
                .connectionString(Environment.getEventHubsConnectionString(), Environment.getEventHubName())
                .consumerGroup(Environment.getConsumerGroup())
                .buildAsyncConsumerClient();

        Disposable subscription = client.receiveFromPartition("0", EventPosition.earliest()).take(50)
                .subscribe(PARTITION_PROCESSOR, error -> {
                    System.err.println("Error: " + error);
                }, () -> {
                    System.out.println("Completed.");
                });

        System.out.println("Starting client");

        System.out.println("Press enter to stop.");
        System.in.read();
        subscription.dispose();

        System.out.println("Stopping client");
        client.close();
        System.out.println("Exiting process");
    }

    public static final Consumer<PartitionEvent> PARTITION_PROCESSOR = eventContext -> {
        PartitionContext partitionContext = eventContext.getPartitionContext();
        EventData eventData = eventContext.getData();

        System.out.printf("partitionId[%s] sequence[%s] Processing event.%n",
                partitionContext.getPartitionId(), eventData.getSequenceNumber());
    };
}
