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
package io.aiven.kafka.connect.opensearch;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;

import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import io.aiven.kafka.connect.opensearch.spi.ClientsConfiguratorProvider;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record HttpClientConfigCallback(
        OpenSearchSinkConnectorConfig config) implements ApacheHttpClient5TransportBuilder.HttpClientConfigCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientConfigCallback.class);

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(final HttpAsyncClientBuilder httpClientBuilder) {
        final var configurators = ClientsConfiguratorProvider.forOpensearch(config);
        configurators.forEach(configurator -> {
            if (configurator.apply(config, httpClientBuilder)) {
                LOGGER.debug("Successfully applied {} configurator to OpensearchClient",
                        configurator.getClass().getName());
            }
        });

        httpClientBuilder.setConnectionManager(createConnectionManager())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build());

        return httpClientBuilder;
    }

    private PoolingAsyncClientConnectionManager createConnectionManager() {
        final var maxPerRoute = Math.max(10, config.maxInFlightRequests() * 2);
        return PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(ClientTlsStrategyBuilder.create()
                        .setSslContext(sslContext(config))
                        .setHostnameVerifier(config.disableHostnameVerification()
                                ? NoopHostnameVerifier.INSTANCE
                                : HttpsSupport.getDefaultHostnameVerifier())
                        .setTlsVersions(config.sslEnableProtocols())
                        .setCiphers(config.cipherSuitesConfig())
                        .buildAsync())
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(config.connectionTimeoutMs(), TimeUnit.MILLISECONDS)
                        .setSocketTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build())
                .setMaxConnPerRoute(maxPerRoute)
                .setMaxConnTotal(maxPerRoute * config.httpHosts().length)
                .build();
    }

    private SSLContext sslContext(final OpenSearchSinkConnectorConfig config) {
        final var sslContextBuilder = SSLContextBuilder.create().setProtocol(config.sslProtocol());
        config.trustStorePath().ifPresentOrElse(p -> {
            try {
                sslContextBuilder.setKeyStoreType(config.trustStoreType())
                        .loadTrustMaterial(p, config.trustStorePassword());
            } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e) {
                throw new ConnectException("Unable to load trust store from " + p, e);
            }
        }, () -> {
            try {
                LOGGER.warn("Hostname verification disabled. Not recommended for production environments.");
                sslContextBuilder.loadTrustMaterial(TrustAllStrategy.INSTANCE);
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
