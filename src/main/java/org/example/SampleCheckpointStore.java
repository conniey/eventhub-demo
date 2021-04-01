package org.example;

import com.azure.core.util.CoreUtils;
import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SampleCheckpointStore implements CheckpointStore {
    private static final String OWNERSHIP = "ownership";
    private static final String SEPARATOR = "/";
    private static final String CHECKPOINT = "checkpoint";
    private final Map<String, PartitionOwnership> partitionOwnershipMap = new ConcurrentHashMap<>();
    private final Map<String, Checkpoint> checkpointsMap = new ConcurrentHashMap<>();
    private final ClientLogger logger = new ClientLogger(SampleCheckpointStore.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Flux<PartitionOwnership> listOwnership(String fullyQualifiedNamespace, String eventHubName,
            String consumerGroup) {
//        logger.info("Listing partition ownership");

        String prefix = prefixBuilder(fullyQualifiedNamespace, eventHubName, consumerGroup, OWNERSHIP);
        return Flux.fromIterable(partitionOwnershipMap.keySet())
                .filter(key -> key.startsWith(prefix))
                .map(key -> partitionOwnershipMap.get(key));
    }

    private String prefixBuilder(String fullyQualifiedNamespace, String eventHubName, String consumerGroup,
            String type) {
        return (fullyQualifiedNamespace +
                SEPARATOR +
                eventHubName +
                SEPARATOR +
                consumerGroup +
                SEPARATOR +
                type)
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Returns a {@link Flux} of partition ownership details for successfully claimed partitions. If a partition is
     * already claimed by an instance or if the ETag in the request doesn't match the previously stored ETag, then
     * ownership claim is denied.
     *
     * @param requestedPartitionOwnerships List of partition ownerships this instance is requesting to own.
     * @return Successfully claimed partition ownerships.
     */
    @Override
    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {
        if (CoreUtils.isNullOrEmpty(requestedPartitionOwnerships)) {
            return Flux.empty();
        }
        PartitionOwnership firstEntry = requestedPartitionOwnerships.get(0);
        String prefix = prefixBuilder(firstEntry.getFullyQualifiedNamespace(), firstEntry.getEventHubName(),
                firstEntry.getConsumerGroup(), OWNERSHIP);

        return Flux.fromIterable(requestedPartitionOwnerships)
                .filter(partitionOwnership -> {
                    return !partitionOwnershipMap.containsKey(partitionOwnership.getPartitionId())
                            || partitionOwnershipMap.get(partitionOwnership.getPartitionId()).getETag()
                            .equals(partitionOwnership.getETag());
                })
//                .doOnNext(partitionOwnership -> logger
//                        .info("Ownership of partition {} claimed by {}", partitionOwnership.getPartitionId(),
//                                partitionOwnership.getOwnerId()))
                .map(partitionOwnership -> {
                    partitionOwnership.setETag(UUID.randomUUID().toString())
                            .setLastModifiedTime(System.currentTimeMillis());
                    partitionOwnershipMap.put(prefix + SEPARATOR + partitionOwnership.getPartitionId(), partitionOwnership);
                    return partitionOwnership;
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Flux<Checkpoint> listCheckpoints(String fullyQualifiedNamespace, String eventHubName,
            String consumerGroup) {
        String prefix = prefixBuilder(fullyQualifiedNamespace, eventHubName, consumerGroup, CHECKPOINT);
        return Flux.fromIterable(checkpointsMap.keySet())
                .filter(key -> key.startsWith(prefix))
                .map(key -> checkpointsMap.get(key));
    }

    /**
     * Updates the in-memory storage with the provided checkpoint information.
     *
     * @param checkpoint The checkpoint containing the information to be stored in-memory.
     * @return A {@link Mono} that completes when the checkpoint is updated.
     */
    @Override
    public Mono<Void> updateCheckpoint(Checkpoint checkpoint) {
        if (checkpoint == null) {
            return Mono.error(logger.logExceptionAsError(new NullPointerException("checkpoint cannot be null")));
        }

        String prefix = prefixBuilder(checkpoint.getFullyQualifiedNamespace(), checkpoint.getEventHubName(),
                checkpoint.getConsumerGroup(), CHECKPOINT);
        checkpointsMap.put(prefix + SEPARATOR + checkpoint.getPartitionId(), checkpoint);
//        logger.info("Updated checkpoint for partition {} with sequence number {}", checkpoint.getPartitionId(),
//                checkpoint.getSequenceNumber());
        return Mono.empty();
    }
}