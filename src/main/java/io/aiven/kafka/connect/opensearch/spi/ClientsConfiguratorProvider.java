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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.ServiceLoader;

import io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig;

public final class ClientsConfiguratorProvider {
    private ClientsConfiguratorProvider() {
    }

    /**
     * Use {@link ServiceLoader} mechanism to discover available configurators for Opensearch (and possibly others)
     * clients which are applicable to the provided configuration.
     *
     * @param config
     *            provided configuration
     * @return the list of discovered {@link OpensearchClientConfigurator} configurators which are applicable to the
     *         provided configuration.
     */
    public static Collection<OpensearchClientConfigurator> forOpensearch(final OpensearchSinkConnectorConfig config) {
        final Collection<OpensearchClientConfigurator> configurators = new ArrayList<>();
        final ServiceLoader<OpensearchClientConfigurator> loaders = ServiceLoader
                .load(OpensearchClientConfigurator.class, ClientsConfiguratorProvider.class.getClassLoader());

        final Iterator<OpensearchClientConfigurator> iterator = loaders.iterator();
        while (iterator.hasNext()) {
            final OpensearchClientConfigurator configurator = iterator.next();
            configurators.add(configurator);
        }

        return configurators;
    }
}
