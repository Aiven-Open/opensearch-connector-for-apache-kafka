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

package io.aiven.kafka.connect.opensearch;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to compute the retry times for a given attempt, using exponential backoff.
 *
 * <p>The purposes of using exponential backoff is to give the ES service time to recover when it
 * becomes overwhelmed. Adding jitter attempts to prevent a thundering herd, where large numbers
 * of requests from many tasks overwhelm the ES service, and without randomization all tasks
 * retry at the same time. Randomization should spread the retries out and should reduce the
 * overall time required to complete all attempts.
 * See <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">this blog post</a>
 * for details.
 */
public class RetryUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * An arbitrary absolute maximum practical retry time.
     */
    public static final long MAX_RETRY_TIME_MS = TimeUnit.HOURS.toMillis(24);

    /**
     * Compute the time to sleep using exponential backoff with jitter. This method computes the
     * normal exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, and then
     * chooses a random value between 0 and that value.
     *
     * @param retryAttempts         the number of previous retry attempts; must be non-negative
     * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to
     *                              be 0 if value is negative
     * @return the non-negative time in milliseconds to wait before the next retry attempt,
     *         or 0 if {@code initialRetryBackoffMs} is negative
     */
    public static long computeRandomRetryWaitTimeInMillis(final int retryAttempts,
                                                          final long initialRetryBackoffMs) {
        if (initialRetryBackoffMs < 0) {
            return 0;
        }
        if (retryAttempts < 0) {
            return initialRetryBackoffMs;
        }
        final long maxRetryTime = computeRetryWaitTimeInMillis(retryAttempts, initialRetryBackoffMs);
        return ThreadLocalRandom.current().nextLong(0, maxRetryTime);
    }

    /**
     * Compute the time to sleep using exponential backoff. This method computes the normal
     * exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, bounded to always
     * be less than {@link #MAX_RETRY_TIME_MS}.
     *
     * @param retryAttempts         the number of previous retry attempts; must be non-negative
     * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to be 0
     *                              if value is negative
     * @return the non-negative time in milliseconds to wait before the next retry attempt,
     *         or 0 if {@code initialRetryBackoffMs} is negative
     */
    public static long computeRetryWaitTimeInMillis(final int retryAttempts,
                                                    final long initialRetryBackoffMs) {
        if (initialRetryBackoffMs < 0) {
            return 0;
        }
        if (retryAttempts <= 0) {
            return initialRetryBackoffMs;
        }
        if (retryAttempts > 32) {
            // This would overflow the exponential algorithm ...
            return MAX_RETRY_TIME_MS;
        }
        final long result = initialRetryBackoffMs << retryAttempts;
        return result < 0L ? MAX_RETRY_TIME_MS : Math.min(MAX_RETRY_TIME_MS, result);
    }

    public static <T> T callWithRetry(final String callName,
                                      final Callable<T> callable,
                                      final int maxRetries,
                                      final long retryBackoffMs) {
        final var time = Time.SYSTEM;
        final int maxAttempts = maxRetries + 1;
        for (int attempts = 1, retryAttempts = 0; true; ++attempts, ++retryAttempts) {
            try {
                LOGGER.trace("Try {} with attempt {}/{}", callName, attempts, maxAttempts);
                return callable.call();
            } catch (final Exception e) {
                if (attempts < maxAttempts) {
                    final long sleepTimeMs = computeRandomRetryWaitTimeInMillis(retryAttempts, retryBackoffMs);
                    LOGGER.warn("Failed to {} with attempt {}/{}, will attempt retry after {} ms. Failure reason: {}",
                            callName, attempts, maxAttempts, sleepTimeMs, e);
                    time.sleep(sleepTimeMs);
                } else {
                    final var msg = String.format(
                            "Failed to %s of %s records after total of {} attempt(s)",
                            callName,
                            attempts
                    );
                    LOGGER.error(msg, e);
                    throw new ConnectException(msg, e);
                }
            }
        }
    }

}
