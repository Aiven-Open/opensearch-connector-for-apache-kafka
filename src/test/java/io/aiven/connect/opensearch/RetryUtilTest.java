/*
 * Copyright 2020 Aiven Oy
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.opensearch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryUtilTest {

    @Test
    public void computeRetryBackoffForValidRanges() {
        assertComputeRetryInRange(10, 10L);
        assertComputeRetryInRange(10, 100L);
        assertComputeRetryInRange(10, 1000L);
        assertComputeRetryInRange(100, 1000L);
    }

    @Test
    public void computeRetryBackoffForNegativeRetryTimes() {
        assertComputeRetryInRange(1, -100L);
        assertComputeRetryInRange(10, -100L);
        assertComputeRetryInRange(100, -100L);
    }

    @Test
    public void computeNonRandomRetryTimes() {
        assertEquals(100L, RetryUtil.computeRetryWaitTimeInMillis(0, 100L));
        assertEquals(200L, RetryUtil.computeRetryWaitTimeInMillis(1, 100L));
        assertEquals(400L, RetryUtil.computeRetryWaitTimeInMillis(2, 100L));
        assertEquals(800L, RetryUtil.computeRetryWaitTimeInMillis(3, 100L));
        assertEquals(1600L, RetryUtil.computeRetryWaitTimeInMillis(4, 100L));
        assertEquals(3200L, RetryUtil.computeRetryWaitTimeInMillis(5, 100L));
    }

    protected void assertComputeRetryInRange(final int retryAttempts, final long retryBackoffMs) {
        for (int i = 0; i != 20; ++i) {
            for (int retries = 0; retries <= retryAttempts; ++retries) {
                final long maxResult = RetryUtil.computeRetryWaitTimeInMillis(retries, retryBackoffMs);
                final long result = RetryUtil.computeRandomRetryWaitTimeInMillis(retries, retryBackoffMs);
                if (retryBackoffMs < 0) {
                    assertEquals(0, result);
                } else {
                    assertTrue(result >= 0L);
                    assertTrue(result <= maxResult);
                }
            }
        }
    }
}
