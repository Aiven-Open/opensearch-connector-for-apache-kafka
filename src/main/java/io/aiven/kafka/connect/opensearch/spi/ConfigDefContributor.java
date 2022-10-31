/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.opensearch.spi;

import java.util.ServiceLoader;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Allow to enrich the connector's configuration with custom configuration definitions 
 * using {@link ServiceLoader} mechanism to discover them.
 */
public interface ConfigDefContributor {
    /**
     * Contribute custom configuration definitions to the connector's configuration.
     * @param config connector's configuration.
     */
    void addConfig(final ConfigDef config);
}
