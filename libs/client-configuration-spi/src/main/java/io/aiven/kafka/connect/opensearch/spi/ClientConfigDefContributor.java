/*
 * Copyright 2020 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.spi;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Allow to enrich the connector's configuration with custom configuration definitions using {@link ServiceLoader}
 * mechanism to discover them.
 */
public interface ClientConfigDefContributor {

    final class DefaultClientConfigDefContributor implements ClientConfigDefContributor {
        @Override
        public void addConfig(final ConfigDef configDef) {
            configDef.define(CONNECTION_URL_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    // If value is null default validator for required value is triggered.
                    if (value != null) {
                        @SuppressWarnings("unchecked") final var urls = (List<String>) value;
                        for (final var url : urls) {
                            try {
                                new URL(url);
                            } catch (final MalformedURLException e) {
                                throw new ConfigException(CONNECTION_URL_CONFIG, url);
                            }
                        }
                    }
                }

                @Override
                public String toString() {
                    return String.join(", ", "http://eshost1:9200", "http://eshost2:9200");
                }
            }, ConfigDef.Importance.HIGH, CONNECTION_URL_DOC, "Connector", 0, ConfigDef.Width.LONG, "Connection URLs");
        }
    }

    String CONNECTION_URL_CONFIG = "connection.url";

    String CONNECTION_URL_DOC = "List of OpenSearch HTTP connection URLs e.g. ``http://eshost1:9200,"
            + "http://eshost2:9200``.";

    static HttpHost[] httpHosts(final AbstractConfig config) {
        final var connectionUrls = connectionUrls(config);
        final var httpHosts = new HttpHost[connectionUrls.size()];
        int idx = 0;
        for (final var url : connectionUrls) {
            httpHosts[idx] = HttpHost.create(url);
            idx++;
        }
        return httpHosts;
    }

    static List<String> connectionUrls(final AbstractConfig config) {
        return config.getList(CONNECTION_URL_CONFIG).stream()
                .map(u -> u.endsWith("/") ? u.substring(0, u.length() - 1) : u)
                .collect(Collectors.toList());
    }

    static void addSpiConfigs(final ConfigDef configDef) {
        final ServiceLoader<ClientConfigDefContributor> loaders = ServiceLoader.load(ClientConfigDefContributor.class,
                OpenSearchClientConfigurator.class.getClassLoader());
        loaders.forEach(clientConfigDefContributor -> clientConfigDefContributor.addConfig(configDef));
    }

    /**
     * Contribute custom configuration definitions to the connector's configuration.
     *
     * @param configDef
     *            connector's configuration.
     */
    void addConfig(final ConfigDef configDef);


}
