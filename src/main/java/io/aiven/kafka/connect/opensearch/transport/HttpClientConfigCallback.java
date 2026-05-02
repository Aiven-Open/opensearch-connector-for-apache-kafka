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

import javax.net.ssl.SSLContext;

import java.util.concurrent.TimeUnit;

import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;

public record HttpClientConfigCallback(SSLContext sslContext,
        OpenSearchSinkConnectorConfig config) implements ApacheHttpClient5TransportBuilder.HttpClientConfigCallback {

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(final HttpAsyncClientBuilder httpClientBuilder) {
        config.usernamePasswordCredentials().ifPresent(credentials -> {
            final var credentialsProvider = new BasicCredentialsProvider();
            for (final var httpHost : config.httpHosts()) {
                credentialsProvider.setCredentials(new AuthScope(httpHost), credentials);
            }
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        });
        httpClientBuilder.setConnectionManager(createConnectionManager())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build());

        return httpClientBuilder;
    }

    private PoolingAsyncClientConnectionManager createConnectionManager() {
        return PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(ClientTlsStrategyBuilder.create()
                        .setSslContext(sslContext)
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
                .setMaxConnPerRoute(config.maxConnPerRoute())
                .setMaxConnTotal(config.maxConnTotal())
                .build();
    }
}
