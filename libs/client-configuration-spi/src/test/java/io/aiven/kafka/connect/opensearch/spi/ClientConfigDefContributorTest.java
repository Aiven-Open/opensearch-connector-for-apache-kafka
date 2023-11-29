package io.aiven.kafka.connect.opensearch.spi;

import io.aiven.kafka.connect.opensearch.spi.ClientConfigDefContributor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.aiven.kafka.connect.opensearch.spi.ClientConfigDefContributor.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.spi.ClientConfigDefContributor.addSpiConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientConfigDefContributorTest {

    public static class CustomConfigDefContributor implements ClientConfigDefContributor {
        @Override
        public void addConfig(ConfigDef configDef) {
            configDef.define("custom.property.1", ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM,
                            "Custom string property 1 description", "custom", 0, ConfigDef.Width.SHORT, "Custom string property 1")
                    .define("custom.property.2", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                            "Custom string property 2 description", "custom", 1, ConfigDef.Width.SHORT,
                            "Custom string property 2");
        }
    }


    public static class SomeConfig extends AbstractConfig {

        static ConfigDef CONFIG;
        static {
            CONFIG = new ConfigDef();
            addSpiConfigs(CONFIG);
        }

        public SomeConfig(Map<String, ?> props) {
            super(CONFIG, props);
        }
    }

    @Test
    public void testThrowsConfigExceptionForWrongUrls() {
        final Map<String, String> props = new HashMap<>();
        props.put(CONNECTION_URL_CONFIG, "ttp://asdsad");
        assertThrows(ConfigException.class, () -> new SomeConfig(props));
    }

    @Test
    public void testAddConfigDefs() {
        final SomeConfig config = new SomeConfig(
                Map.of(
                        CONNECTION_URL_CONFIG, "http://localhost",
                        "custom.property.1", "10",
                        "custom.property.2", "http://localhost:9000"));
        assertEquals(config.getInt("custom.property.1"), 10);
        assertEquals(config.getString("custom.property.2"), "http://localhost:9000");
    }


}
