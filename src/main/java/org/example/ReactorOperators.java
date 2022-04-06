package org.example;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReactorOperators {
    private static final AtomicInteger PARTITION_0 = new AtomicInteger();
    private static final AtomicLong END_TIME = new AtomicLong();
    private static final int schedulerSize = Runtime.getRuntime().availableProcessors() * 4;
    private static final int MAXIMUM_QUEUE_SIZE = 10000;

    public static void main(String[] args) throws InterruptedException {
        System.out.printf("Batch Size: %d. Window: %s. Use window? %b%n", Environment.getBatchSize(),
                Environment.getWindowTimeout(), Environment.useWindowTimeout());

        final Scheduler scheduler = Schedulers.newBoundedElastic(schedulerSize,
                MAXIMUM_QUEUE_SIZE, "partition-pump");
        final Flux<Integer> range = Flux.range(0, Integer.MAX_VALUE - 1);
        final Flux<Flux<Integer>> windowedRange;
        if (Environment.useWindowTimeout()) {
            windowedRange = range.windowTimeout(Environment.getBatchSize(), Environment.getWindowTimeout());
        } else {
            windowedRange = range.window(Environment.getBatchSize());
        }

        final Flux<List<Integer>> listFlux = windowedRange.concatMap(Flux::collectList)
                .publishOn(scheduler, false, 500);

        System.out.println("Starting event processor");

        final long startTime = System.currentTimeMillis();

        final Disposable subscribe = listFlux.subscribe(partitionEventBatch -> {
                            PARTITION_0.addAndGet(partitionEventBatch.size());
                        },
                        /* EventHubConsumer receive() returned an error */
                        ex -> {
                            System.err.printf("Error! %s%n.", ex);
                        },
                        () -> {
                            System.out.println("Completed window range.");
                            if (END_TIME.compareAndSet(0, System.currentTimeMillis())) {
                                System.out.println("Setting end time in disposable.");
                            }
                        });

        // Continue to perform other tasks while the processor is running in the background.
        Thread.sleep(TimeUnit.MINUTES.toMillis(5));

        System.out.println("Stopping event processor");
        subscribe.dispose();

        final long endTime = System.currentTimeMillis();

        if (!END_TIME.compareAndSet(0, endTime)) {
            System.out.println("End-time was already set.");
        }

        final long durationInMs = END_TIME.get() - startTime;
        final double durationInS = (durationInMs) * 0.001;
        final int p0 = PARTITION_0.get();

        System.out.printf("Start: [%d] End: [%d]. P0 [%d]. P1 [%d].%n", startTime, endTime, p0, 0);
        System.out.printf("P0: [%f] events/s%n", (p0 / durationInS));

        System.out.println("Exiting process");
        scheduler.dispose();
    }
}
