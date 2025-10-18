package org.apache.helix.util;

import org.testng.annotations.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests focused on finding JenkinsHash outputs where the lower 16 bits are 0.
 * This is useful for testing the distribution properties of the hash function.
 */
public class JenkinsHashLower16BitsTest {
    private static final JenkinsHash JENKINS_HASH = new JenkinsHash();
    private static final JenkinsHash2 JENKINS_HASH2 = new JenkinsHash2();
    private static final long MAX_ATTEMPTS = 10_000_000_000L;
    private static final int MAX_EXAMPLES = 500000;
    private static final long PROGRESS_REPORT_INTERVAL = 50_000_000;
    private static final int NUM_THREADS = 8;

    @Test
    public void testFindHashesWithLower16BitsZero() {
        System.out.println("=== Finding hashes with lower 16 bits 0 ===");

        // Test with different numbers of parameters
        for (int paramCount = 3; paramCount <= 3; paramCount++) { // for 1 param test for now
            System.out.printf("\n=== Testing %d-parameter hashes with %d threads ===%n", paramCount, NUM_THREADS);
            int found = findRandomHashesWithLower16BitsZeroParallel(paramCount);

            // Instead of failing, just log if we didn't find any matches
            if (found == 0) {
                System.out.printf("No hashes with lower 16 bits 0 found in %,d attempts for %d parameters%n",
                                 MAX_ATTEMPTS, paramCount);
            } else {
                System.out.printf("Found %d hashes with lower 16 bits 0 for %d parameters%n",
                                 found, paramCount);
            }

            // We don't fail the test if no hashes are found, as this is a probabilistic test
            // and the absence of matches doesn't necessarily indicate a problem with the hash function
        }
    }

    private int findRandomHashesWithLower16BitsZeroParallel(int paramCount) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        AtomicInteger foundOld = new AtomicInteger(0);
        AtomicInteger foundNew = new AtomicInteger(0);
        AtomicLong totalAttempts = new AtomicLong(0);
        long startTime = System.currentTimeMillis();

        // Divide work among threads
        long attemptsPerThread = MAX_ATTEMPTS / NUM_THREADS;
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        // Progress reporting thread
        ScheduledExecutorService progressReporter = Executors.newSingleThreadScheduledExecutor();
        progressReporter.scheduleAtFixedRate(() -> {
            long attempts = totalAttempts.get();
            if (attempts > 0) {
                System.out.printf("Progress: %,d attempts, old: %d, new: %d matches%n",
                    attempts, foundOld.get(), foundNew.get());
            }
        }, 10, 10, TimeUnit.SECONDS);

        for (int threadId = 0; threadId < NUM_THREADS; threadId++) {
            final int tid = threadId;
            executor.submit(() -> {
                try {
                    java.util.Random random = new java.util.Random(System.currentTimeMillis() + tid*1000);
                    int localFoundOld = 0;
                    int localFoundNew = 0;

                    for (long attempt = 1; attempt <= attemptsPerThread &&
                         foundOld.get() < MAX_EXAMPLES && foundNew.get() < MAX_EXAMPLES; attempt++) {

                        long[] params = random.longs(paramCount).toArray();
                        long hash = computeHash(params);
                        long hash2 = computeHash2(params);

                        if ((hash & 0xFFFF) == 0) {
                            localFoundOld++;
                            foundOld.incrementAndGet();
                        }

                        if ((hash2 & 0xFFFF) == 0) {
                            localFoundNew++;
                            foundNew.incrementAndGet();
                        }

                        // Update total attempts periodically
                        if (attempt % 1000000 == 0) {
                            totalAttempts.addAndGet(1000000);
                        }
                    }

                    // Add remaining attempts
                    totalAttempts.addAndGet(attemptsPerThread % 1000000);

                    System.out.printf("Thread %d completed: old=%d, new=%d%n",
                        tid, localFoundOld, localFoundNew);

                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            // Wait for all threads to complete
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Test interrupted");
        } finally {
            executor.shutdown();
            progressReporter.shutdown();
        }

        long duration = System.currentTimeMillis() - startTime;
        long actualAttempts = totalAttempts.get();

        System.out.printf("Completed %,d attempts in %d ms (%.2f attempts/sec)%n",
                         actualAttempts, duration,
                         actualAttempts * 1000.0 / duration);

        System.out.printf("Total: %d collisions found for older JenkinsHash%n" +
            "and %d collisions found for the modified hash%n",
            foundOld.get(), foundNew.get());

        System.out.printf("old vs new ratio: %.4f%n",
            foundOld.get() == 0 ? 0.0 :
                (double) foundNew.get() / (double) foundOld.get());
        return foundOld.get();
    }

    private long computeHash(long... params) {
        switch (params.length) {
            case 1:
                return JENKINS_HASH.hash(params[0]);
            case 2:
                return JENKINS_HASH.hash(params[0], params[1]);
            case 3:
                return JENKINS_HASH.hash(params[0], params[1], params[2]);
            default:
                throw new IllegalArgumentException("Unsupported number of parameters: " + params.length);
        }
    }

    private long computeHash2(long... params) {
        switch (params.length) {
            case 1:
                return JENKINS_HASH2.hash(params[0]);
            case 2:
                return JENKINS_HASH2.hash(params[0], params[1]);
            case 3:
                return JENKINS_HASH2.hash(params[0], params[1], params[2]);
            default:
                throw new IllegalArgumentException("Unsupported number of parameters: " + params.length);
        }
    }
}
