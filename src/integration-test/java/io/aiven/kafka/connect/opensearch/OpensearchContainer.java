/*
 * Copyright 2021 Aiven Oy
 * Copyright 2016 Confluent Inc.
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

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

public class OpensearchContainer extends GenericContainer<OpensearchContainer> {

    private static final int DEFAULT_HTTP_PORT = 9200;

    private static final int DEFAULT_TCP_PORT = 9600;

    private static final String DEFAULT_VERSION = "1.1.0";

    private static final long ONE_GIGABYTES = 1024 * 1024 * 1024;

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("opensearchproject/opensearch").withTag(DEFAULT_VERSION);

    public OpensearchContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public OpensearchContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        logger().info("Starting an Opensearch container using [{}]", dockerImageName);
        withNetworkAliases(String.format("opensearch-%s", Base58.randomString(6)));
        withEnv("discovery.type", "single-node");
        withLogConsumer(this::opensearchLog);
        withSharedMemorySize(ONE_GIGABYTES);
        addExposedPorts(DEFAULT_HTTP_PORT, DEFAULT_TCP_PORT);
        setWaitStrategy(
                new LogMessageWaitStrategy()
                        .withRegEx(".*(Node '\\w+' initialized).*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(2))
        );
    }

    protected void opensearchLog(final OutputFrame logMessage) {
        switch (logMessage.getType()) {
            case STDOUT:
                // Normal output in yellow
                System.out.print((char) 27 + "[33m" + logMessage.getUtf8String());
                System.out.print((char) 27 + "[0m"); // reset
                break;
            case STDERR:
                // Error output in red
                System.err.print((char) 27 + "[31m" + logMessage.getUtf8String());
                System.out.print((char) 27 + "[0m"); // reset
                break;
            case END:
                // End output in green
                System.err.print((char) 27 + "[32m" + logMessage.getUtf8String());
                System.out.print((char) 27 + "[0m"); // reset
                break;
            default:
                break;
        }
    }

    public String getHttpHostAddress() {
        return String.format("https://%s:%s", getHost(), getMappedPort(DEFAULT_HTTP_PORT));
    }

}
