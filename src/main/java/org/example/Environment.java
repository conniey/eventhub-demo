package org.example;

import com.azure.messaging.eventhubs.EventHubClientBuilder;

/**
 * Environment variables used for run.
 */
class Environment {
    /**
     * Gets Event Hubs namespace connection string.
     *
     * @return Event Hubs namespace connection string.
     */
    static String getEventHubsConnectionString() {
        return System.getenv("EVENTHUB_CONNECTION_STRING");
    }

    /**
     * Gets the name of the Event Hub.
     *
     * @return Name of the Event Hub.
     */
    static String getEventHubName() {
        return System.getenv("EVENTHUB_NAME");
    }

    /**
     * Gets the consumer group.
     *
     * @return The consumer group or the default one if none is set.
     */
    static String getConsumerGroup() {
        final String consumerGroup = System.getenv("CONSUMER_GROUP");
        return consumerGroup == null ? EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME : consumerGroup;
    }

    /**
     * Gets the storage container name.
     *
     * @return The storage container name.
     */
    static String getStorageContainerName() {
        return System.getenv("STORAGE_CONTAINER_NAME");
    }

    /**
     * Get the storage connection string.
     *
     * @return The storage connection string.
     */
    static String getStorageConnectionString() {
        return System.getenv("STORAGE_CONNECTION_STRING");
    }

    static boolean runForever() {
        return System.getenv("FOREVER") != null;
    }

    /**
     * Static class.
     */
    private Environment() {
    }
}
