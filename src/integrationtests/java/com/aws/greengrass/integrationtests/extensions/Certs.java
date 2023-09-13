/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.clientdevices.auth.exception.CertificateGenerationException;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.Utils;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Stream;

public class Certs {
    private static final Duration CERT_EXPIRY = Duration.ofMinutes(5);

    @Getter
    private final String serverKeystorePassword = Utils.generateRandomString(20);

    private final KeyPair caKeys;
    private final X509Certificate caCert;
    private final KeyPair clientKeyPair;
    private final X509Certificate clientCert;
    private final KeyPair serverKeyPair;
    private final X509Certificate serverCert;
    private final KeyStore serverKeyStore;
    private final KeyStore serverTrustStore;

    public Certs(MQTTClientKeyStore clientKeyStore) throws KeyStoreException {
        try {
            this.caKeys = genKeys();
            this.caCert = genCACert(caKeys);

            this.serverKeyPair = genKeys();
            this.serverCert = genServerCert();
            this.serverKeyStore = createServerKeystore();
            this.serverTrustStore = createServerTruststore();

            this.clientKeyPair = genKeys();
            this.clientCert = genClientCert();
            trustClientCert();

            initClientKeyStoreWithCerts(clientKeyStore);
        } catch (CertificateException | IOException | OperatorCreationException
                 | NoSuchAlgorithmException | CertificateGenerationException e) {
            throw new KeyStoreException(e);
        }
    }

    @Data
    @Builder
    public static class RotationResult {
        KeyPair kp;
        X509Certificate cert;
        X509Certificate[] caCerts;
    }

    public RotationResult rotateClientCert() throws Exception {
        genClientCert();
        trustClientCert();
        return RotationResult.builder()
                .kp(clientKeyPair)
                .cert(clientCert)
                .caCerts(new X509Certificate[]{caCert})
                .build();
    }

    private X509Certificate genCACert(KeyPair keyPair)
            throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, CertIOException {
        Instant now = Instant.now();
        return CertificateHelper.createCACertificate(
                keyPair,
                Date.from(now),
                Date.from(now.plus(CERT_EXPIRY)),
                "localhost"
        );
    }

    public KeyPair genKeys() throws NoSuchAlgorithmException {
        return CertificateStore.newRSAKeyPair(4096);
    }

    private X509Certificate genServerCert()
            throws CertificateException, NoSuchAlgorithmException, IOException, OperatorCreationException {
        Instant now = Instant.now();
        return CertificateHelper.issueServerCertificate(
                caCert,
                caKeys.getPrivate(),
                CertificateHelper.getX500Name("localhost"),
                serverKeyPair.getPublic(),
                Collections.singletonList("localhost"),
                Date.from(now),
                Date.from(now.plus(CERT_EXPIRY)));
    }

    private X509Certificate genClientCert()
            throws CertificateException, NoSuchAlgorithmException, IOException, OperatorCreationException {
        Instant now = Instant.now();
        return CertificateHelper.issueClientCertificate(
                caCert,
                caKeys.getPrivate(),
                CertificateHelper.getX500Name("client"),
                clientKeyPair.getPublic(),
                Date.from(now),
                Date.from(now.plus(CERT_EXPIRY)));
    }

    private KeyStore createServerKeystore() throws KeyStoreException {
        KeyStore serverKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());

        // create empty keystore
        try {
            serverKeyStore.load(null, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        // add key and certs to keystore
        serverKeyStore.setKeyEntry(
                "hivemq",
                serverKeyPair.getPrivate(),
                serverKeystorePassword.toCharArray(),
                Stream.of(serverCert, caCert).toArray(X509Certificate[]::new)
        );

        return serverKeyStore;
    }

    private KeyStore createServerTruststore() throws KeyStoreException {
        KeyStore serverTruststore = KeyStore.getInstance(KeyStore.getDefaultType());

        // create empty keystore
        try {
            serverTruststore.load(null, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        return serverTruststore;
    }

    private void trustClientCert() throws KeyStoreException {
        serverTrustStore.setCertificateEntry(
                "client",
                clientCert
        );
    }

    private MQTTClientKeyStore initClientKeyStoreWithCerts(MQTTClientKeyStore clientKeyStore)
            throws KeyStoreException, CertificateException, IOException, CertificateGenerationException {
        clientKeyStore.init();
        clientKeyStore.updateCert(new CertificateUpdateEvent(clientKeyPair, clientCert, new X509Certificate[]{caCert}));
        clientKeyStore.updateCA(Collections.singletonList(CertificateHelper.toPem(caCert)));
        return clientKeyStore;
    }

    public void writeServerKeystore(Path output) throws KeyStoreException {
        try (OutputStream fos = Files.newOutputStream(output)) {
            serverKeyStore.store(fos, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new KeyStoreException("unable to write keystore to " + output, e);
        }
    }

    public void writeServerTruststore(Path output) throws KeyStoreException {
        try (OutputStream fos = Files.newOutputStream(output)) {
            serverTrustStore.store(fos, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new KeyStoreException("unable to write truststore to " + output, e);
        }
    }
}
