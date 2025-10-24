/*
 * Copyright 2024 Aiven Oy
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

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;
import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.regions.Region;

/**
 * Adds AWS SigV4 authentication to the {@link HttpAsyncClientBuilder} for OpenSearch client if configured. This enables
 * authentication to Amazon OpenSearch Service (ES) and Amazon OpenSearch Serverless (AOSS) using IAM credentials from
 * the AWS SDK DefaultCredentialsProvider chain (supports IRSA, env vars, profiles).
 */
public class OpensearchAwsAuthConfigurator implements OpensearchClientConfigurator, ConfigDefContributor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpensearchAwsAuthConfigurator.class);

    public static final String AWS_AUTH_REGION_CONFIG = "aws.auth.region";
    private static final String AWS_AUTH_REGION_DOC = "The AWS region for SigV4 signing (e.g., us-west-2). "
            + "Required for AWS authentication.";

    public static final String AWS_AUTH_SERVICE_NAME_CONFIG = "aws.auth.service.name";
    private static final String AWS_AUTH_SERVICE_NAME_DOC = "The AWS service name for SigV4 signing. "
            + "Use 'aoss' for Amazon OpenSearch Serverless or 'es' for Amazon OpenSearch Service. "
            + "Required for AWS authentication.";

    @Override
    public boolean apply(final OpensearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        if (!isAwsAuthConfigured(config)) {
            return false;
        }

        final String region = config.getString(AWS_AUTH_REGION_CONFIG);
        final String serviceName = config.getString(AWS_AUTH_SERVICE_NAME_CONFIG);

        LOGGER.info("Configuring AWS SigV4 authentication with region={}, service={}", region, serviceName);

        try {
            // Configure AwsV4HttpSigner to always include payload hash (required for AOSS)
            final AwsV4HttpSigner signer = AwsV4HttpSigner.create();

            final HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(serviceName, signer,
                    DefaultCredentialsProvider.create(), Region.of(region));

            builder.addInterceptorLast(interceptor);

            LOGGER.info("AWS SigV4 authentication configured successfully for service={}, region={}", serviceName,
                    region);
            return true;
        } catch (final Exception e) {
            LOGGER.error("Failed to configure AWS SigV4 authentication", e);
            throw new RuntimeException("Failed to configure AWS SigV4 authentication", e);
        }
    }

    @Override
    public void addConfig(final ConfigDef config) {
        config.define(AWS_AUTH_REGION_CONFIG, Type.STRING, null, Importance.MEDIUM, AWS_AUTH_REGION_DOC,
                "AWS Authentication", 0, Width.SHORT, "AWS Region")
                .define(AWS_AUTH_SERVICE_NAME_CONFIG, Type.STRING, null, Importance.MEDIUM, AWS_AUTH_SERVICE_NAME_DOC,
                        "AWS Authentication", 1, Width.SHORT, "AWS Service Name");
    }

    private static boolean isAwsAuthConfigured(final OpensearchSinkConnectorConfig config) {
        final boolean configured = Objects.nonNull(config.getString(AWS_AUTH_REGION_CONFIG))
                && Objects.nonNull(config.getString(AWS_AUTH_SERVICE_NAME_CONFIG));

        if (configured) {
            LOGGER.debug("AWS authentication is configured");
        } else {
            LOGGER.debug("AWS authentication is not configured");
        }

        return configured;
    }
}
