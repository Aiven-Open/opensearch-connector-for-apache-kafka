/*
 * Copyright 2019 Aiven Oy
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

import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;
import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

import org.apache.hc.core5.http.HttpHost;
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
public class OpenSearchSigV4ConfigDefContributor implements ConfigDefContributor {
    // AssumeRole request limit details here:
    // https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
    public static final int STS_ROLE_MIN_SESSION_DURATION = 900;
    public static final int STS_ROLE_MAX_SESSION_DURATION = 43_200;
    public static final int STS_ROLE_DURATION_DEFAULT = 3600;

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access_key_id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret_access_key";

    public static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access key id";
    public static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key";

    public static final String AWS_STS_ROLE_ARN_CONFIG = "aws.sts.role.arn";
    public static final String AWS_STS_ROLE_EXTERNAL_ID_CONFIG = "aws.sts.role.external.id";
    public static final String AWS_STS_ROLE_SESSION_NAME_CONFIG = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION_CONFIG = "aws.sts.role.session.duration";

    public static final String AWS_REGION_CONFIG = "aws.region";
    private static final String AWS_REGION_DOC = "AWS Region, e.g. us-east-1. This field is required to enable AWS SigV4 request signing";
    public static final String AWS_SERVICE_SIGNING_NAME_CONFIG = "aws.service.signing.name";
    public static final String AWS_SERVICE_SIGNING_NAME_DOC = "AWS Service Signing Name, eg es. This field is required to enable AWS SigV4 request signing";

    private static final String AWS_ACCESS_GROUP_NAME = "AWS Access";
    private static final String AWS_STS_GROUP_NAME = "AWS STS Access";
    private static final String AWS_SIG4_GROUP_NAME = "AWS Sig4 Authentication";

    @Override
    public void addConfig(final ConfigDef config) {
        addAwsAccessGroup(config);
        addAwsStsGroup(config);
        addAwsSig4Group(config);
    }

    private void addAwsAccessGroup(final ConfigDef config) {
        config.define(AWS_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC, AWS_ACCESS_GROUP_NAME, 0, ConfigDef.Width.SHORT, "Access Key Id")
                .define(AWS_SECRET_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        AWS_SECRET_ACCESS_KEY_DOC, AWS_ACCESS_GROUP_NAME, 1, ConfigDef.Width.SHORT,
                        "Secret Access Key");
    }

    private void addAwsStsGroup(final ConfigDef config) {
        int awsStsGroupCounter = 0;
        config.define(AWS_STS_ROLE_ARN_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS Role", AWS_STS_GROUP_NAME, ++awsStsGroupCounter,
                ConfigDef.Width.NONE, AWS_STS_ROLE_ARN_CONFIG)
                .define(AWS_STS_ROLE_SESSION_NAME_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM, "AWS STS Session name", AWS_STS_GROUP_NAME, ++awsStsGroupCounter,
                        ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_NAME_CONFIG)
                .define(AWS_STS_ROLE_SESSION_DURATION_CONFIG, ConfigDef.Type.INT, STS_ROLE_DURATION_DEFAULT,
                        ConfigDef.Range.between(STS_ROLE_MIN_SESSION_DURATION, STS_ROLE_MAX_SESSION_DURATION),
                        ConfigDef.Importance.MEDIUM, "AWS STS Session duration", AWS_STS_GROUP_NAME,
                        ++awsStsGroupCounter, ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_DURATION_CONFIG)
                .define(AWS_STS_ROLE_EXTERNAL_ID_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM, "AWS STS External Id", AWS_STS_GROUP_NAME, ++awsStsGroupCounter,
                        ConfigDef.Width.NONE, AWS_STS_ROLE_EXTERNAL_ID_CONFIG);
    }

    private void addAwsSig4Group(final ConfigDef config) {
        config.define(AWS_REGION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, AWS_REGION_DOC,
                AWS_SIG4_GROUP_NAME, 0, ConfigDef.Width.SHORT, "AWS Region")
                .define(AWS_SERVICE_SIGNING_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        AWS_SERVICE_SIGNING_NAME_DOC, AWS_SIG4_GROUP_NAME, 0, ConfigDef.Width.SHORT,
                        "AWS Service Signing Name");
    }

    public static boolean configured(final OpenSearchSinkConnectorConfig config) {
        return hasBasicCredentialsSettings(config) || hasStsCredentialsSettings(config);
    }

    public static void validateSettings(final OpenSearchSinkConnectorConfig config) {
        if (hasBasicCredentialsSettings(config) && hasStsCredentialsSettings(config))
            throw new ConfigException("Found both AWS access and STS role credentials. Only one can be selected");
        final var region = config.getString(AWS_REGION_CONFIG);
        if (region == null || region.isEmpty()) {
            throw new ConfigException(AWS_REGION_CONFIG, config.getString(AWS_REGION_CONFIG));
        }
        final var serviceName = config.getString(AWS_SERVICE_SIGNING_NAME_CONFIG);
        if (serviceName == null || serviceName.isEmpty()) {
            throw new ConfigException(AWS_SERVICE_SIGNING_NAME_CONFIG, serviceName);
        }
        final var connUrls = config.connectionUrls();
        if (connUrls.size() > 1) {
            throw new ConfigException(AWS_SERVICE_SIGNING_NAME_CONFIG,
                    Arrays.stream(config.httpHosts()).map(HttpHost::toString).collect(Collectors.joining(",")),
                    "Only one OpenSearch endpoint, without scheme should be provided");
        }
        if (URI.create(connUrls.get(0)).getScheme() != null) {
            throw new ConfigException(AWS_SERVICE_SIGNING_NAME_CONFIG, config.httpHosts()[0].toString(),
                    "Only one OpenSearch endpoint, without scheme should be provided");
        }
    }

    public static AwsCredentialsProvider createAwsCredentialsProvider(final OpenSearchSinkConnectorConfig config) {
        if (hasBasicCredentialsSettings(config)) {
            return StaticCredentialsProvider
                    .create(AwsBasicCredentials.create(config.getString(AWS_ACCESS_KEY_ID_CONFIG),
                            config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value()));
        } else if (hasStsCredentialsSettings(config)) {
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder().region(Region.of(config.getString(AWS_REGION_CONFIG))).build())
                    .refreshRequest(AssumeRoleRequest.builder()
                            .roleArn(config.getString(AWS_STS_ROLE_ARN_CONFIG))
                            .roleSessionName(config.getString(AWS_STS_ROLE_SESSION_NAME_CONFIG))
                            .durationSeconds(config.getInt(AWS_STS_ROLE_SESSION_DURATION_CONFIG))
                            .externalId(config.getString(AWS_STS_ROLE_EXTERNAL_ID_CONFIG))
                            .build())
                    .build();
        }
        throw new ConfigException("Couldn't configure AWS credentials provider");
    }

    private static boolean hasBasicCredentialsSettings(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_ACCESS_KEY_ID_CONFIG) != null
                && config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG) != null;
    }

    private static boolean hasStsCredentialsSettings(final OpenSearchSinkConnectorConfig config) {
        return config.getString(AWS_STS_ROLE_ARN_CONFIG) != null
                && config.getString(AWS_STS_ROLE_SESSION_NAME_CONFIG) != null;
    }

}
