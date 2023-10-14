/*
 * Copyright 2019 Aiven Oy
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

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;
import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;


/**
 * Adds AWS SigV4 authentication to the {@index HttpAsyncClientBuilder} for Opensearch client
 * if configured.
 */
public class OpensearchSigV4Configurator
    implements OpensearchClientConfigurator, ConfigDefContributor {

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access_key_id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret_access_key";
    public static final String AWS_REGION_CONFIG = "aws.region";
    private static final String AWS_ACCESS_KEY_ID_DOC =
        "AWS Access key id, this field is required "
            + "to enable AWS SigV4 request signing";
    private static final String AWS_SECRET_ACCESS_KEY_DOC =
        "AWS secret access key, this field is required "
            + "to enable AWS SigV4 request signing";
    private static final String AWS_REGION_DOC =
        "AWS Region, eg us-east-1. This field is required "
            + "to enable AWS SigV4 request signing";

    private static final String AWS_GROUP_NAME = "AWS Authentication SigV4";

    private static boolean isAuthenticatedConnection(final OpensearchSinkConnectorConfig config) {
        return Objects.nonNull(awsAccessKeyId(config))
            && Objects.nonNull(awsSecretAccessKey(config))
            && Objects.nonNull(awsRegion(config));
    }

    private static String awsRegion(final OpensearchSinkConnectorConfig config) {
        return config.getString(AWS_REGION_CONFIG);
    }

    private static String awsAccessKeyId(final OpensearchSinkConnectorConfig config) {
        return config.getString(AWS_ACCESS_KEY_ID_CONFIG);
    }

    private static Password awsSecretAccessKey(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG);
    }

    @Override
    public boolean apply(final OpensearchSinkConnectorConfig config,
                         final HttpAsyncClientBuilder builder) {
        if (!isAuthenticatedConnection(config)) {
            return false;
        }

        final AwsCredentials credentials = AwsBasicCredentials.create(
            awsAccessKeyId(config),
            awsSecretAccessKey(config).value());
        final StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

        final HttpRequestInterceptor awsSignerInterceptor = new AwsRequestSigningApacheInterceptor(
            "es",
            Aws4Signer.create(),
            credentialsProvider,
            Region.of(config.getString(AWS_REGION_CONFIG))
        );

        builder.addInterceptorLast(awsSignerInterceptor);

        return true;
    }

    @Override
    public void addConfig(final ConfigDef config) {
        config
            .define(
                AWS_ACCESS_KEY_ID_CONFIG,
                Type.STRING,
                null,
                Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC,
                AWS_GROUP_NAME,
                0,
                Width.SHORT,
                "Access Key Id"
            ).define(
                AWS_SECRET_ACCESS_KEY_CONFIG,
                Type.PASSWORD,
                null,
                Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC,
                AWS_GROUP_NAME,
                1,
                Width.SHORT,
                "Secret Access Key"
            ).define(
                AWS_REGION_CONFIG,
                Type.STRING,
                null,
                Importance.MEDIUM,
                AWS_REGION_DOC,
                AWS_GROUP_NAME,
                1,
                Width.SHORT,
                "Region");
    }
}
