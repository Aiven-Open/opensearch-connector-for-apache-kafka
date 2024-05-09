/*
 * Copyright 2024 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.auth;

import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_KEY_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_KEY_PASSWORD;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_CA_CERTIFICATE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_PASSWORD;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_TYPE;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_PROTOCOL_TYPE;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_TRUSTSTORE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_TRUSTSTORE_PASSWORD;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig;

import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.ssl.SSLContexts;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

class SSLContextBuilder {

    static final String TLS_1_3 = "TLSv1.3";

    static final String TLS_1_2 = "TLSv1.2";

    static final String[] SUPPORTED_PROTOCOLS = { TLS_1_2, TLS_1_3 };

    public static Optional<SSLContext> buildSSLContext(final OpensearchSinkConnectorConfig connectorConfig) {
        if (!hasKeystoreAndTruststoreSettings(connectorConfig) && !hasPemCertificatesSettings(connectorConfig)) {
            return Optional.empty();
        }
        if (hasKeystoreAndTruststoreSettings(connectorConfig) && hasPemCertificatesSettings(connectorConfig)) {
            throw new ConfigException(
                    "One of Keystore and Truststore files or X.509 PEM certificates and PKCS#8 keys groups should be set");
        }
        try {
            final var trustStore = buildTrustStore(connectorConfig);
            final var keyStore = buildKeyStore(connectorConfig);
            return Optional.of(SSLContexts.custom()
                    .loadTrustMaterial(trustStore, TrustAllStrategy.INSTANCE)
                    .loadKeyMaterial(keyStore, accessKeyPassword(connectorConfig).value().toCharArray())
                    .setProtocol(sslProtocol(connectorConfig))
                    .build());
        } catch (final NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException
                | KeyManagementException e) {
            throw new ConfigException("Couldn't build SSL context", e);
        }
    }

    private static KeyStore buildTrustStore(final OpensearchSinkConnectorConfig connectorConfig) {
        try {
            if (hasPemCertificatesSettings(connectorConfig)) {
                final var certificatesChain = readX509Certificates(Path.of(caCertificateLocation(connectorConfig)));
                final var trustStore = createKeyStore("JKS");
                for (var i = 0; i < certificatesChain.length; i++) {
                    String alias = Integer.toString(i);
                    trustStore.setCertificateEntry(alias, certificatesChain[i]);
                }
                return trustStore;
            } else if (hasKeystoreAndTruststoreSettings(connectorConfig)) {
                return createKeyStore("JKS", Path.of(trustStoreLocation(connectorConfig)),
                        trustStorePassword(connectorConfig).value().toCharArray());
            }
            return null;
        } catch (final IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new ConfigException("Couldn't build trust store", e);
        }
    }

    private static KeyStore buildKeyStore(final OpensearchSinkConnectorConfig connectorConfig) {
        try {
            if (hasPemCertificatesSettings(connectorConfig)) {
                final var certificatesChain = readX509Certificates(Path.of(accessCertificateLocation(connectorConfig)));
                final var privetKey = readPrivateKey(Path.of(accessKeyLocation(connectorConfig)),
                        accessKeyPassword(connectorConfig));
                final var keystore = createKeyStore("JKS");
                keystore.setKeyEntry("access_key", privetKey, accessKeyPassword(connectorConfig).value().toCharArray(),
                        certificatesChain);
                return keystore;
            } else if (hasKeystoreAndTruststoreSettings(connectorConfig)) {
                return createKeyStore(keyStoreType(connectorConfig), Path.of(keyStoreLocation(connectorConfig)),
                        keyStorePassword(connectorConfig).value().toCharArray());
            }
            return null;
        } catch (final IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new ConfigException("Couldn't build key store", e);
        }
    }

    private static String sslProtocol(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_PROTOCOL_TYPE);
    }

    private static KeyStore createKeyStore(final String type)
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
        return createKeyStore(type, null, null);
    }

    private static KeyStore createKeyStore(final String type, final Path location, final char[] password)
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        final var keyStore = KeyStore.getInstance(type);
        if (isNull(location)) {
            keyStore.load(null, null);
        } else {
            try (final var in = Files.newInputStream(location)) {
                keyStore.load(in, password);
            }
        }
        return keyStore;
    }

    private static boolean hasPemCertificatesSettings(final OpensearchSinkConnectorConfig config) {
        return nonNull(caCertificateLocation(config)) || nonNull(accessCertificateLocation(config))
                || nonNull(accessKeyLocation(config));
    }

    private static boolean hasKeystoreAndTruststoreSettings(final OpensearchSinkConnectorConfig config) {
        return nonNull(trustStoreLocation(config)) || nonNull(keyStoreLocation(config));
    }

    private static String caCertificateLocation(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_CA_CERTIFICATE_LOCATION);
    }

    private static String accessCertificateLocation(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION);
    }

    private static String accessKeyLocation(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_ACCESS_KEY_LOCATION);
    }

    private static Password accessKeyPassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(CLIENT_SSL_ACCESS_KEY_PASSWORD);
    }

    private static String trustStoreLocation(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_TRUSTSTORE_LOCATION);
    }

    private static Password trustStorePassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(CLIENT_SSL_TRUSTSTORE_PASSWORD);
    }

    private static String keyStoreType(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_KEYSTORE_TYPE);
    }

    private static String keyStoreLocation(final OpensearchSinkConnectorConfig config) {
        return config.getString(CLIENT_SSL_KEYSTORE_LOCATION);
    }

    private static Password keyStorePassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(CLIENT_SSL_KEYSTORE_PASSWORD);
    }

    private static PEMParser createPEMParser(final Path location) throws IOException {
        return new PEMParser(Files.newBufferedReader(location));
    }

    private static X509Certificate[] readX509Certificates(final Path location) {
        final var certificates = new ArrayList<X509Certificate>();
        final var converter = new JcaX509CertificateConverter();
        try (final var pemParser = createPEMParser(location)) {
            Object pemObject;
            while ((pemObject = pemParser.readObject()) != null) {
                if (pemObject instanceof X509CertificateHolder) {
                    certificates.add(converter.getCertificate((X509CertificateHolder) pemObject));
                }
            }
        } catch (final IOException ioe) {
            throw new ConfigException("Couldn't read PEM content from " + location);
        } catch (final CertificateException ce) {
            throw new ConfigException("Couldn't get X.509 certificate from " + location);
        }
        return certificates.toArray(new X509Certificate[0]);
    }

    private static PrivateKey readPrivateKey(final Path location, final Password keyPassword) {
        final var converter = new JcaPEMKeyConverter();
        try (final var pemParser = createPEMParser(location)) {
            Object pemObject;
            while ((pemObject = pemParser.readObject()) != null) {
                if (isNull(keyPassword)) {
                    if (pemObject instanceof PrivateKeyInfo) {
                        return converter.getPrivateKey((PrivateKeyInfo) pemObject);
                    } else if (pemObject instanceof PEMKeyPair) {
                        return converter.getKeyPair((PEMKeyPair) pemObject).getPrivate();
                    } else {
                        throw new ConfigException("Unable to parse PEM object {} as a non encrypted key", location);
                    }
                } else if (pemObject instanceof PEMEncryptedKeyPair) {
                    final var provider = new JcePEMDecryptorProviderBuilder().build(keyPassword.value().toCharArray());
                    return converter.getKeyPair(((PEMEncryptedKeyPair) pemObject).decryptKeyPair(provider))
                            .getPrivate();
                } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                    final var provider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                            .build(keyPassword.value().toCharArray());
                    return converter
                            .getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(provider));
                }
            }
        } catch (final IOException ioe) {
            throw new ConfigException("Couldn't read PEM content from " + location, ioe);
        } catch (final PKCSException | OperatorCreationException e) {
            throw new ConfigException("Couldn't get private key from " + location, e);
        }
        throw new ConfigException("Couldn't get private key from " + location);
    }

}
