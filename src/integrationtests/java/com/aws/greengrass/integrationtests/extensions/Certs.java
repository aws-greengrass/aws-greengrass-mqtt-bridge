/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.clientdevices.auth.exception.CertificateGenerationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.Utils;
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
    private static final Logger LOGGER = LogManager.getLogger(Certs.class);
    private static final Duration CERT_EXPIRY = Duration.ofMinutes(5);

    @Getter
    private final String serverKeystorePassword = Utils.generateRandomString(20);

    private KeyPair caKeys;
    private X509Certificate caCert;
    private KeyPair clientKeyPair;
    private X509Certificate clientCert;
    private KeyPair serverKeyPair;
    private X509Certificate serverCert;
    private KeyStore serverKeyStore;
    private KeyStore serverTrustStore;
    private final MQTTClientKeyStore clientKeyStore;
    private final Path keystorePath;
    private final Path trustorePath;

    public Certs(MQTTClientKeyStore clientKeyStore,
                 Path keystorePath,
                 Path trustorePath) {
        this.clientKeyStore = clientKeyStore;
        this.keystorePath = keystorePath;
        this.trustorePath = trustorePath;
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void initialize() throws KeyStoreException {
        try {
            this.caKeys = genKeys();
            this.caCert = genCACert();

            this.serverKeyPair = genKeys();
            this.serverCert = genServerCert();
            this.serverKeyStore = createServerKeystore();
            this.serverTrustStore = createServerTruststore();
            storeServerKeyEntry();

            this.clientKeyPair = genKeys();
            this.clientCert = genClientCert();
            trustClientCert();

            initClientKeyStoreWithCerts();

            writeServerKeystore();
            writeServerTruststore();
        } catch (Exception e) {
            throw new KeyStoreException(e);
        }
    }

    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void rotateClientCert() throws Exception {
        this.clientCert = genClientCert();
        trustClientCert();
        clientKeyStore.updateCert(new CertificateUpdateEvent(clientKeyPair, clientCert, new X509Certificate[]{caCert}));
        writeServerTruststore();
    }

    public void waitForBrokerToApplyStoreChanges() throws InterruptedException {
        // TODO see if we can improve this
        Thread.sleep(Duration.ofSeconds(30).toMillis());
    }

    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void rotateServerCert() throws Exception {
        this.serverCert = genServerCert();
        storeServerKeyEntry();
        writeServerKeystore();
    }

    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void rotateCA() throws Exception {
        this.caCert = genCACert();
        clientKeyStore.updateCA(Collections.singletonList(CertificateHelper.toPem(caCert)));
        writeServerKeystore();
    }

    private X509Certificate genCACert()
            throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, CertIOException {
        Instant now = Instant.now();
        return CertificateHelper.createCACertificate(
                caKeys,
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
        X509Certificate cert = CertificateHelper.issueClientCertificate(
                caCert,
                caKeys.getPrivate(),
                CertificateHelper.getX500Name("client"),
                clientKeyPair.getPublic(),
                Date.from(now),
                Date.from(now.plus(CERT_EXPIRY)));
        LOGGER.atInfo()
                .kv("subject", cert.getIssuerX500Principal().getName())
                .kv("notBefore", cert.getNotBefore())
                .kv("notAfter", cert.getNotAfter())
                .kv("serialNumber", cert.getSerialNumber())
                .log("New client certificate generated");
        return cert;
    }

    private KeyStore createServerKeystore() throws KeyStoreException {
        KeyStore serverKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());

        // create empty keystore
        try {
            serverKeyStore.load(null, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        return serverKeyStore;
    }

    private void storeServerKeyEntry() throws KeyStoreException {
        // add key and certs to keystore
        serverKeyStore.setKeyEntry(
                "hivemq",
                serverKeyPair.getPrivate(),
                serverKeystorePassword.toCharArray(),
                Stream.of(serverCert, caCert).toArray(X509Certificate[]::new)
        );
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

    private MQTTClientKeyStore initClientKeyStoreWithCerts()
            throws KeyStoreException, CertificateException, IOException, CertificateGenerationException {
        clientKeyStore.init();
        clientKeyStore.updateCert(new CertificateUpdateEvent(clientKeyPair, clientCert, new X509Certificate[]{caCert}));
        clientKeyStore.updateCA(Collections.singletonList(CertificateHelper.toPem(caCert)));
        return clientKeyStore;
    }

    public void writeServerKeystore() throws KeyStoreException {
        try (OutputStream fos = Files.newOutputStream(keystorePath)) {
            serverKeyStore.store(fos, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new KeyStoreException("unable to write keystore to " + keystorePath, e);
        }
    }

    public void writeServerTruststore() throws KeyStoreException {
        try (OutputStream fos = Files.newOutputStream(trustorePath)) {
            serverTrustStore.store(fos, serverKeystorePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new KeyStoreException("unable to write truststore to " + trustorePath, e);
        }
    }
}
