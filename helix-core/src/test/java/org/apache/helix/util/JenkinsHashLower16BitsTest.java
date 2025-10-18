package org.apache.helix.util;

import org.testng.annotations.Test;

/**
 * Tests focused on finding JenkinsHash outputs where the lower 16 bits are 0.
 * This is useful for testing the distribution properties of the hash function.
 */
public class JenkinsHashLower16BitsTest {
    private static final JenkinsHash JENKINS_HASH = new JenkinsHash();
    private static final JenkinsHash2 JENKINS_HASH2 = new JenkinsHash2();
    private static final long MAX_ATTEMPTS = 1_000_000_000;
    private static final int MAX_EXAMPLES = 500000;
    private static final long PROGRESS_REPORT_INTERVAL = 50_000_000;

    @Test
    public void testFindHashesWithLower16BitsZero() {
        System.out.println("=== Finding hashes with lower 16 bits 0 ===");

        // Test with different numbers of parameters
        for (int paramCount = 3; paramCount <= 3; paramCount++) { // for 1 param test for now
            System.out.printf("\n=== Testing %d-parameter hashes ===%n", paramCount);
            int found = findRandomHashesWithLower16BitsZero(paramCount);

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

    private int findRandomHashesWithLower16BitsZero(int paramCount) {
        java.util.Random random = new java.util.Random();
        int found = 0;
        int found2 = 0;
        long startTime = System.currentTimeMillis();

        for (int attempt = 1; attempt <= MAX_ATTEMPTS && found < MAX_EXAMPLES; attempt++) {
            long[] params = random.longs(paramCount).toArray();
            long hash = computeHash(params);
            long hash2 = computeHash2(params);

            if ((hash & 0xFFFF) == 0) {
                found++;
//                System.out.printf("Found1 %d: %s -> 0x%x%n",
//                                found,
//                                java.util.Arrays.toString(params),
//                                hash);
            }

            if ((hash2 & 0xFFFF) == 0) {
                found2++;
//                System.out.printf("Found2 %d: %s -> 0x%x%n",
//                    found2,
//                    java.util.Arrays.toString(params),
//                    hash2);
            }


            // Print progress every PROGRESS_REPORT_INTERVAL attempts
            if (attempt % PROGRESS_REPORT_INTERVAL == 0) {
                System.out.printf("Attempt %,d: old: %d, new: %d  matches %n",
                                attempt, found, found2 , MAX_EXAMPLES);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Completed %,d attempts in %d ms%n",
                         Math.min(MAX_ATTEMPTS, (found < MAX_EXAMPLES) ? MAX_ATTEMPTS : (found * MAX_ATTEMPTS / MAX_EXAMPLES)),
                         duration);

        System.out.printf("a total of %d collisions found for older JenkinsHash \n"
            + " and %d collision found for the prime based hash", found, found2);

        return found;
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
