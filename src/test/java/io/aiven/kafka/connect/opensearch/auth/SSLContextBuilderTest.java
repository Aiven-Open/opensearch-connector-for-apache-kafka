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

import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_KEY_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_ACCESS_KEY_PASSWORD;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_CA_CERTIFICATE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_PASSWORD;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_KEYSTORE_TYPE;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_TRUSTSTORE_LOCATION;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_TRUSTSTORE_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig;

import org.apache.commons.lang3.RandomStringUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.RFC4519Style;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.PKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.io.pem.PemReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSLContextBuilderTest {

    private final static BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

    static Path CA_CERTIFICATE_PATH;

    static Path USER_ACCESS_CERTIFICATE_PATH;

    static Path USER_ACCESS_KEY_PATH;

    final static String USER_ACCESS_KEY_PASSWORD = RandomStringUtils.randomAlphabetic(10);

    @BeforeAll
    static void setup(final @TempDir Path tmpFolder) throws Exception {
        final var keyPair = generateKeyPair();
        generateCaCertificate(keyPair, tmpFolder);
        generateAccessCertificate(keyPair, tmpFolder);
    }

    private static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", BOUNCY_CASTLE_PROVIDER);
        generator.initialize(4096);
        return generator.generateKeyPair();
    }

    private static void generateCaCertificate(final KeyPair parentKeyPair, final Path tmpFolder)
            throws IOException, NoSuchAlgorithmException, OperatorCreationException {
        CA_CERTIFICATE_PATH = tmpFolder.resolve("ca_root.crt");
        writePemContent(CA_CERTIFICATE_PATH,
                createCertificateBuilder(
                        "DC=org,DC=example,O=Example Org.,OU=Example Org. Root CA,CN=Example Org. Root CA",
                        parentKeyPair.getPublic(), parentKeyPair.getPublic())
                        .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
                        .addExtension(Extension.keyUsage, true,
                                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign | KeyUsage.cRLSign))
                        .build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(BOUNCY_CASTLE_PROVIDER)
                                .build(parentKeyPair.getPrivate())));
    }

    private static void generateAccessCertificate(final KeyPair parentKeyPair, final Path tmpFolder)
            throws NoSuchAlgorithmException, IOException, OperatorCreationException {
        final var keyPair = generateKeyPair();
        USER_ACCESS_CERTIFICATE_PATH = tmpFolder.resolve("user_access.crt");
        writePemContent(USER_ACCESS_CERTIFICATE_PATH,
                createCertificateBuilder("CN=user_access,OU=client,O=client,L=test,C=de", keyPair.getPublic(),
                        parentKeyPair.getPublic())
                        .addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
                        .addExtension(Extension.keyUsage, true,
                                new KeyUsage(
                                        KeyUsage.digitalSignature | KeyUsage.nonRepudiation | KeyUsage.keyEncipherment))
                        .addExtension(Extension.extendedKeyUsage, true,
                                new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth))
                        .build(new JcaContentSignerBuilder("SHA256withRSA").setProvider(BOUNCY_CASTLE_PROVIDER)
                                .build(parentKeyPair.getPrivate())));
        USER_ACCESS_KEY_PATH = tmpFolder.resolve("user_access.key");
        writePemContent(USER_ACCESS_KEY_PATH,
                new PKCS8Generator(PrivateKeyInfo.getInstance(keyPair.getPrivate().getEncoded()),
                        new JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.PBE_SHA1_3DES).setRandom(new SecureRandom())
                                .setPassword(USER_ACCESS_KEY_PASSWORD.toCharArray())
                                .build())
                        .generate());
    }

    private static void writePemContent(final Path path, final Object pemContent) throws IOException {
        try (JcaPEMWriter writer = new JcaPEMWriter(Files.newBufferedWriter(path))) {
            writer.writeObject(pemContent);
        }
    }

    private static X509v3CertificateBuilder createCertificateBuilder(final String subject,
            final PublicKey certificatePublicKey, final PublicKey parentPublicKey)
            throws NoSuchAlgorithmException, CertIOException {
        final var issuerName = new X500Name(RFC4519Style.INSTANCE, subject);
        final var subjectName = new X500Name(RFC4519Style.INSTANCE, subject);
        final var startDate = Instant.now().minusMillis(24 * 3600 * 1000);
        final var endDate = Instant.from(startDate).plus(10, ChronoUnit.DAYS);
        final var serialNumber = BigInteger.valueOf(Instant.now().plusMillis(100).getEpochSecond());
        final var extUtils = new JcaX509ExtensionUtils();
        return new X509v3CertificateBuilder(issuerName, serialNumber, Date.from(startDate), Date.from(endDate),
                subjectName, SubjectPublicKeyInfo.getInstance(certificatePublicKey.getEncoded()))
                .addExtension(Extension.authorityKeyIdentifier, false,
                        extUtils.createAuthorityKeyIdentifier(parentPublicKey))
                .addExtension(Extension.subjectKeyIdentifier, false,
                        extUtils.createSubjectKeyIdentifier(certificatePublicKey));
    }

    @Test
    public void shouldFailIfBothSSLConfigGroupsWereSet() {
        final var config = new OpensearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://127.0.0.1",
                CLIENT_SSL_CA_CERTIFICATE_LOCATION, "a", CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION, "b",
                CLIENT_SSL_ACCESS_KEY_LOCATION, "c", CLIENT_SSL_ACCESS_KEY_PASSWORD, "d",
                CLIENT_SSL_TRUSTSTORE_LOCATION, "e", CLIENT_SSL_TRUSTSTORE_PASSWORD, "f", CLIENT_SSL_KEYSTORE_LOCATION,
                "g", CLIENT_SSL_KEYSTORE_TYPE, "h", CLIENT_SSL_KEYSTORE_PASSWORD, "i"));
        assertThrows(ConfigException.class, () -> SSLContextBuilder.buildSSLContext(config));
    }

    @Test
    public void shouldSkipSSLContextConfigIfNothingSet() {
        final var config = new OpensearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://127.0.0.1"));
        assertTrue(SSLContextBuilder.buildSSLContext(config).isEmpty());
    }

    @Test
    public void shouldBuildSSLContextUsingPemFiles() {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(CONNECTION_URL_CONFIG, "http://127.0.0.1", CLIENT_SSL_CA_CERTIFICATE_LOCATION,
                        CA_CERTIFICATE_PATH.toString(), CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION,
                        USER_ACCESS_CERTIFICATE_PATH.toString(), CLIENT_SSL_ACCESS_KEY_LOCATION,
                        USER_ACCESS_KEY_PATH.toString(), CLIENT_SSL_ACCESS_KEY_PASSWORD, USER_ACCESS_KEY_PASSWORD));
        assertTrue(SSLContextBuilder.buildSSLContext(config).isPresent());
    }

    @Test
    public void shouldBuildSSLContextUsingTrustAndKeyStoreFiles(final @TempDir Path tmpFolder) throws Exception {
        final var trustStorePath = tmpFolder.resolve("truststore.jks");
        final var trustStorePassword = RandomStringUtils.randomAlphabetic(10);

        final var trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", x509Certificate(CA_CERTIFICATE_PATH));
        trustStore.store(Files.newOutputStream(trustStorePath), trustStorePassword.toCharArray());

        final var keyStorePath = tmpFolder.resolve("keystore.jks");
        final var keyStorePassword = RandomStringUtils.randomAlphabetic(10);
        final var keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry("key", privateKey(USER_ACCESS_KEY_PATH), USER_ACCESS_KEY_PASSWORD.toCharArray(),
                new Certificate[] { x509Certificate(USER_ACCESS_CERTIFICATE_PATH) });
        keyStore.store(Files.newOutputStream(keyStorePath), keyStorePassword.toCharArray());

        final var config = new OpensearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://127.0.0.1",
                CLIENT_SSL_TRUSTSTORE_LOCATION, trustStorePath.toString(), CLIENT_SSL_TRUSTSTORE_PASSWORD,
                trustStorePassword, CLIENT_SSL_KEYSTORE_LOCATION, keyStorePath.toString(), CLIENT_SSL_KEYSTORE_PASSWORD,
                keyStorePassword, CLIENT_SSL_ACCESS_KEY_PASSWORD, USER_ACCESS_KEY_PASSWORD));

        assertTrue(SSLContextBuilder.buildSSLContext(config).isPresent());
    }

    private X509Certificate x509Certificate(final Path path) throws IOException, CertificateException {
        try (final var pemReader = new PemReader(Files.newBufferedReader(path))) {
            final var pemContent = pemReader.readPemObject().getContent();
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(pemContent));
        }
    }

    private PrivateKey privateKey(final Path path) throws IOException, PKCSException, OperatorCreationException {
        try (final var parser = new PEMParser(Files.newBufferedReader(path))) {
            final var pemObject = parser.readObject();
            final var provider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                    .build(USER_ACCESS_KEY_PASSWORD.toCharArray());
            return new JcaPEMKeyConverter()
                    .getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(provider));
        }
    }

}
