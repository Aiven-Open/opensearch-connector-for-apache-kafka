/*
 * Copyright 2023 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.sig4;

import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_ARN_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_EXTERNAL_ID_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_DURATION_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_NAME_CONFIG;
import static java.util.Objects.nonNull;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;
import io.aiven.kafka.connect.opensearch.spi.OpenSearchClientConfigurator;

import joptsimple.internal.Strings;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Adds AWS SigV4 authentication to the {@index HttpAsyncClientBuilder} for OpenSearch client if configured.
 */
public class OpenSearchSigV4ClientConfigurator implements OpenSearchClientConfigurator {

    @Override
    public boolean isAuthenticatedConnection(final OpenSearchSinkConnectorConfig config) {
        return hasBasicCredentialsSettings(config) || hasStsCredentialsSettings(config);
    }

    private String accessKeyId(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_ACCESS_KEY_ID_CONFIG);
    }

    private String secretAccessKey(final OpenSearchSinkConnectorConfig config) {
        final var pwd = config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG);
        if (nonNull(pwd))
            return pwd.value();
        return null;
    }

    private String region(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_REGION_CONFIG);
    }

    private String serviceSigningName(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_SERVICE_SIGNING_NAME_CONFIG);
    }

    private boolean hasBasicCredentialsSettings(final OpenSearchSinkConnectorConfig config) {
        return nonNull(accessKeyId(config)) && nonNull(secretAccessKey(config));
    }

    private String stsRoleArn(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_STS_ROLE_ARN_CONFIG);
    }

    private String stsSessionName(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_STS_ROLE_SESSION_NAME_CONFIG);
    }

    private int stsSessionDuration(final OpenSearchSinkConnectorConfig config) {
        return config.getInt(AWS_STS_ROLE_SESSION_DURATION_CONFIG);
    }

    private boolean hasStsCredentialsSettings(final OpenSearchSinkConnectorConfig config) {
        return nonNull(stsRoleArn(config)) && nonNull(stsSessionName(config));
    }

    private void validateSig4SignatureSettings(final OpenSearchSinkConnectorConfig config) {
        try {
            Region.of(region(config));
        } catch (final Exception e) {
            throw new ConfigException(AWS_REGION_CONFIG, region(config));
        }
        if (Strings.isNullOrEmpty(serviceSigningName(config))) {
            throw new ConfigException(AWS_SERVICE_SIGNING_NAME_CONFIG, region(config));
        }
    }

    private AwsCredentialsProvider credentialsProvider(final OpenSearchSinkConnectorConfig config) {
        if (hasBasicCredentialsSettings(config)) {
            return StaticCredentialsProvider
                    .create(AwsBasicCredentials.create(accessKeyId(config), secretAccessKey(config)));
        } else if (hasStsCredentialsSettings(config)) {
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder().region(Region.of(region(config))).build())
                    .refreshRequest(AssumeRoleRequest.builder()
                            .roleArn(stsRoleArn(config))
                            .roleSessionName(stsSessionName(config))
                            .durationSeconds(stsSessionDuration(config))
                            .externalId(config.getString(AWS_STS_ROLE_EXTERNAL_ID_CONFIG))
                            .build())
                    .build();
        }
        throw new ConfigException("Couldn't configure AWS credentials provider");
    }

    @Override
    public boolean apply(final OpenSearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        if (!isAuthenticatedConnection(config)) {
            return false;
        }
        if (hasBasicCredentialsSettings(config) && hasStsCredentialsSettings(config))
            throw new ConfigException("Found both AWS access and STS role credentials. Only one can be selected.");
        validateSig4SignatureSettings(config);

        builder.addRequestInterceptorLast(new Sig4HttpRequestInterceptor(credentialsProvider(config), region(config),
                serviceSigningName(config)));
        return true;
    }

}
