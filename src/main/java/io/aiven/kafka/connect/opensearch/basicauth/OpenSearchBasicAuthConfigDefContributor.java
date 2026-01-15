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
package io.aiven.kafka.connect.opensearch.basicauth;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

public class OpenSearchBasicAuthConfigDefContributor implements ConfigDefContributor {

    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    private static final String CONNECTION_USERNAME_DOC = "The username used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "The password used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";

    @Override
    public void addConfig(ConfigDef config) {
        config.define(CONNECTION_USERNAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                CONNECTION_USERNAME_DOC, "Authentication", 0, ConfigDef.Width.SHORT, "Connection Username")
                .define(CONNECTION_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        CONNECTION_PASSWORD_DOC, "Authentication", 1, ConfigDef.Width.SHORT, "Connection Password");
    }
}
