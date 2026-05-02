/*
 * Copyright 2026 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.transport;

import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.errors.ConnectException;

import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;
import software.amazon.awssdk.regions.Region;

public final class TransportProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportProvider.class);

    private TransportProvider() {
    }

    public static OpenSearchTransport createTransport(final OpenSearchSinkConnectorConfig config) {
        final var sslContext = sslContext(config);
        return config.awsCredentialsProvider()
                .map(credentials -> buildAwsTransport(sslContext, credentials, config))
                .orElseGet(() -> buildApacheHttpClientTransport(sslContext, config));
    }

    private static OpenSearchTransport buildAwsTransport(final SSLContext sslContext,
            final AwsCredentialsProvider credentials, final OpenSearchSinkConnectorConfig config) {
        final var region = config.getString(AWS_REGION_CONFIG);
        final var serviceSigningName = config.getString(AWS_SERVICE_SIGNING_NAME_CONFIG);
        return new AwsSdk2Transport(
                ApacheHttpClient.builder()
                        .connectionTimeout(Duration.of(config.connectionTimeoutMs(), ChronoUnit.MILLIS))
                        .socketTimeout(Duration.of(config.readTimeoutMs(), ChronoUnit.MILLIS))
                        .maxConnections(config.maxConnTotal())
                        .socketFactory(new SdkTlsSocketFactory(sslContext,
                                config.trustAllCertificates()
                                        ? NoopHostnameVerifier.INSTANCE
                                        : SSLConnectionSocketFactory.getDefaultHostnameVerifier()))
                        .build(),
                config.connectionUrls().getFirst(), serviceSigningName, Region.of(region),
                AwsSdk2TransportOptions.builder().setCredentials(credentials).build());
    }

    private static OpenSearchTransport buildApacheHttpClientTransport(final SSLContext sslContext,
            final OpenSearchSinkConnectorConfig config) {
        return ApacheHttpClient5TransportBuilder.builder(config.httpHosts())
                .setHttpClientConfigCallback(new HttpClientConfigCallback(sslContext, config))
                .build();
    }

    private static SSLContext sslContext(final OpenSearchSinkConnectorConfig config) {
        final var sslContextBuilder = SSLContextBuilder.create().setProtocol(config.sslProtocol());
        config.trustStorePath().ifPresentOrElse(p -> {
            try {
                sslContextBuilder.setKeyStoreType(config.trustStoreType())
                        .loadTrustMaterial(p, config.trustStorePassword());
            } catch (final NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e) {
                throw new ConnectException("Unable to load trust store from " + p, e);
            }
        }, () -> {
            try {
                if (config.trustAllCertificates()) {
                    LOGGER.warn("Hostname verification disabled. Not recommended for production environments.");
                    sslContextBuilder.loadTrustMaterial(TrustAllStrategy.INSTANCE);
                }
            } catch (final NoSuchAlgorithmException | KeyStoreException e) {
                throw new ConnectException("Unable to configure SSL context with trust all strategy", e);
            }
        });
        config.keyStorePath().ifPresent(p -> {
            try {
                sslContextBuilder.setKeyStoreType(config.keyStoreType())
                        .loadKeyMaterial(p, config.keyStorePassword(), config.keyPassword());
            } catch (final NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException
                    | CertificateException | IOException e) {
                throw new ConnectException("Unable to load key store from " + p, e);
            }
        });
        try {
            return sslContextBuilder.build();
        } catch (final NoSuchAlgorithmException | KeyManagementException e) {
            throw new ConnectException("Unable to build SSL context", e);
        }
    }

}
