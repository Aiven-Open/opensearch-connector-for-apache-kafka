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
package io.aiven.kafka.connect.opensearch.spi;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * The extension points to allow OpensearchClient customization based on the the provided configuration.
 */
public interface OpenSearchClientConfigurator {

    /**
     * Apply configurator to the {@link HttpAsyncClientBuilder} instance according to the provided configuration.
     *
     * @param config
     *            provided configuration
     * @param builder
     *            {@link HttpAsyncClientBuilder} instance
     * @return "true" if the configurator is applicable, "false" otherwise
     */
    boolean apply(final AbstractConfig config, final HttpAsyncClientBuilder builder);


}
