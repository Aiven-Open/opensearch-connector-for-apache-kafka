package io.aiven.kafka.connect.opensearch;

import io.aiven.kafka.connect.opensearch.spi.ClientConfigDefContributor;
import org.apache.kafka.common.config.ConfigDef;

public class OpenSearchBasicAuthConfigDefContributor implements ClientConfigDefContributor {

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
        config.define(CONNECTION_USERNAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, CONNECTION_USERNAME_DOC,
                        "Authentication", 0, ConfigDef.Width.SHORT, "Connection Username")
                .define(CONNECTION_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, CONNECTION_PASSWORD_DOC,
                        "Authentication", 1, ConfigDef.Width.SHORT, "Connection Password");
    }

}
